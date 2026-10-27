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

//!
//! Each test opens its Go owner in `pkg/ddl/tests/fk/foreign_key_test.go`;
//! Go's settings are its `set` statements, and its `getTableInfo` reads are
//! `tk.get_table_by_name` against the domain's current schema.

use crate::support::testkit::TestKit;
use tidb_executor::{KvForeignKey, KvTable, TableEntry};

/// Go `getTableInfo(t, dom, db, table)`: the storage-backed table as the
/// domain's current schema holds it.
fn table_info(tk: &TestKit, db: &str, table: &str) -> KvTable {
    match tk.get_table_by_name(db, table) {
        TableEntry::Kv(table) => (*table).clone(),
        other => panic!("expected a storage-backed table {db}.{table}, got {other:?}"),
    }
}

/// Go `getTableInfoReferredForeignKeys`: the constraints in the domain's
/// current schema that name `db.table` as their referenced table (computed
/// on demand here; see the sibling `fk_create_meta_info_source` module doc).
fn referred_foreign_keys(tk: &TestKit, db: &str, table: &str) -> Vec<(String, String, KvForeignKey)> {
    let catalog = tk.info_schema();
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

/// Go `getTableInfo(t, dom, db, table).ForeignKeys`.
fn declared(tk: &TestKit, db: &str, table: &str) -> Vec<KvForeignKey> {
    table_info(tk, db, table).foreign_keys().to_vec()
}

/// Go `getTableInfo(t, dom, db, table).Indices`, by name.
fn index_names(tk: &TestKit, db: &str, table: &str) -> Vec<String> {
    table_info(tk, db, table)
        .indexes()
        .iter()
        .map(|index| index.name.clone())
        .collect()
}

// --- TestAddForeignKey (pkg/ddl/tests/fk/foreign_key_test.go:1077) ---
//
// Go's ALTER-side ladder, re-derived from the Go assertions:
//   * `alter table t2 add foreign key (b) references t1(id)` (parent side
//     covered by the clustered primary key) succeeds and bumps
//     MaxForeignKeyID to 1;
//   * the same against a parent column with NO index fails
//     `infoschema.ErrForeignKeyNoIndexInParent` — the separate test below;
//   * after the parent gains `index(b)`, the constraint lands;
//   * a constraint referencing the table ITSELF fails
//     `infoschema.ErrCannotAddForeign` — also in the separate test below;
//   * `add constraint fk foreign key (b) references t1(b)` auto-creates the
//     support index named fk, and the planner reads t2 through it;
//   * adding THREE constraints in ONE statement lands all of them plus the
//     shared idx_c, in order, all public, and every orphan insert fails
//     `[planner:1452]ErrNoReferencedRow2`;
//   * the SAME multi-add with one bad referenced column adds NOTHING
//     (atomicity — covered by the focused regression below);
//   * the circular-dependency ADD fails
//     `[ddl:1452]Cannot add or update a child row: ...` and leaves neither
//     table's meta touched;
//   * the auto-create-index arm's failure leaves the meta untouched too.
//
// The EXPLAIN leg reads Go's plan tree (`IndexReader` over `index:fk`);
// this port asserts the `use index(fk)` read's (empty) rows.
#[test]
fn alter_add_foreign_key_lands_constraints_and_enforces_child_side() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int)");
    tk.must_exec("create table t2 (id int key, b int)");
    tk.must_exec("alter table t2 add index(b)");
    tk.must_exec("alter table t2 add foreign key (b) references t1(id)");
    assert_eq!(
        table_info(&tk, "test", "t2").max_foreign_key_id(),
        1,
        "Go: tbl2Info.MaxForeignKeyID == 1 after the first add"
    );

    // Go: `alter table t2 add foreign key (b) references t1(b);` fails with
    // ErrForeignKeyNoIndexInParent while t1 has no index on b — pinned by
    // the focused test below.

    // After the parent gains its index, the constraint lands.
    tk.must_exec("alter table t1 add index(b)");
    tk.must_exec("alter table t2 add foreign key (b) references t1(b)");
    assert_eq!(declared(&tk, "test", "t2").len(), 2);

    // Auto-create index for a named constraint, then read through it.
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (id int key, b int, index(b))");
    tk.must_exec("create table t2 (id int key, b int)");
    tk.must_exec("alter table t2 add constraint fk foreign key (b) references t1(b)");
    assert_eq!(
        index_names(&tk, "test", "t2"),
        vec!["fk".to_owned()],
        "Go: len(tbl2Info.Indices) == 1, named fk"
    );
    tk.must_query("select b from t2 use index(fk)")
        .check(Vec::<&str>::new());

    // Three constraints in ONE statement, with their shared index.
    tk.must_exec("alter table t2 add column c int, add column d int, add column e int");
    tk.must_exec("alter table t2 add index idx_c(c, d, e)");
    tk.must_exec(
        "alter table t2 add constraint fk_c foreign key (c) references t1(b), \
         add constraint fk_d foreign key (d) references t1(b), \
         add constraint fk_e foreign key (e) references t1(b)",
    );
    let table = table_info(&tk, "test", "t2");
    assert_eq!(table.indexes().len(), 4, "Go: len(tbl2Info.Indices) == 4");
    let index_names: Vec<String> = table.indexes().iter().map(|index| index.name.clone()).collect();
    assert_eq!(index_names, vec!["fk", "idx_c", "fk_d", "fk_e"], "Go's index order");
    let names: Vec<String> = table.foreign_keys().iter().map(|key| key.name.clone()).collect();
    assert_eq!(names, vec!["fk", "fk_c", "fk_d", "fk_e"], "Go's FK order");

    // Every orphan insert fails [planner:1452] — the parent has no rows.
    for (id, column) in [("1", "b"), ("2", "c"), ("3", "d"), ("4", "e")] {
        tk.must_get_err_code(&format!("insert into t2 (id, {column}) values ({id}, 1)"), 1452);
    }

    // Circular dependency: t1.a=1 has no match in t2, so the ADD fails with
    // 1452 and NEITHER table's meta moves. (Go's message appends `ON DELETE
    // CASCADE`; this tier renders the constraint without the action suffix
    // — the captured rendering divergence from b105.)
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (id int key, a int, index(a))");
    tk.must_exec("create table t2 (id int key, a int, foreign key fk(a) references t1(id) ON DELETE CASCADE)");
    tk.must_exec("insert into t1 values (1, 1)");
    tk.must_get_err_code(
        "alter table t1 add foreign key fk(a) references t2(id) ON DELETE CASCADE",
        1452,
    );
    assert!(declared(&tk, "test", "t1").is_empty(), "Go: len(tbl1Info.ForeignKeys) == 0");
    assert!(referred_foreign_keys(&tk, "test", "t2").is_empty(), "Go: no referred FKs on t2");

    // The auto-create-index arm's failure: same refusal, same untouched
    // meta — this time the child has NO support index to offer.
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (id int key, a int)");
    tk.must_exec("create table t2 (id int key)");
    tk.must_exec("insert into t1 values (1, 1)");
    tk.must_get_err_code(
        "alter table t1 add foreign key fk(a) references t2(id) ON DELETE CASCADE",
        1452,
    );
    assert!(declared(&tk, "test", "t1").is_empty());
    assert!(referred_foreign_keys(&tk, "test", "t2").is_empty());
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
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int)");
    tk.must_exec("create table t2 (id int key, b int, index(b))");

    let error = tk.must_get_err_code("alter table t2 add foreign key (b) references t1(b)", 1822);
    assert_eq!(
        error.message,
        "Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'"
    );
    assert!(declared(&tk, "test", "t2").is_empty());

    tk.must_exec("alter table t1 add index(b)");
    tk.must_exec("alter table t2 add foreign key (b) references t1(b)");

    let error = tk.must_get_err_code("alter table t2 add foreign key (b) references t2(b)", 1215);
    assert_eq!(error.message, "Cannot add foreign key constraint");
    assert_eq!(declared(&tk, "test", "t2").len(), 1);
}

// Go's TestAddForeignKey partition rows (`pkg/ddl/tests/fk/foreign_key_test.go:
// 1427-1435`): ALTER TABLE ... ADD FOREIGN KEY rejects a partitioned parent or
// child with schema 1506 before staging the constraint or its support index.
#[test]
fn alter_add_foreign_key_refuses_partitioning_on_either_side() {
    let cases = [
        (
            "create table t1 (id int key) partition by hash(id) partitions 3;",
            "create table t2 (id int key);",
        ),
        (
            "create table t1 (id int key);",
            "create table t2 (id int key) partition by hash(id) partitions 3;",
        ),
    ];
    for (parent, child) in cases {
        let mut tk = TestKit::new();
        tk.must_exec(parent);
        tk.must_exec(child);
        let error = tk.must_get_err_code(
            "alter table t2 add constraint fk foreign key (id) references t1(id)",
            1506,
        );
        assert_eq!(
            error.message,
            "Foreign key clause is not yet supported in conjunction with partitioning"
        );
    }
}

// The multi-add atomicity rows of Go's TestAddForeignKey
// (pkg/ddl/tests/fk/foreign_key_test.go:1145-1153): one ALTER adding
// fk_c/fk_d/fk_e where fk_e names `t1(unknown_col)` fails with
// `infoschema.ErrForeignKeyNoColumnInParent` and leaves the table with ZERO
// constraints; and `alter table t2 drop index idx_c, add constraint fk_c
// foreign key (c) references t1(b)` fails `ErrDropIndexNeededInForeignKey`
// (1553) because the drop would strand the add.
#[test]
fn a_failed_multi_add_leaves_no_constraint_behind() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int, index(b));");
    tk.must_exec("create table t2 (id int key, c int, d int, e int, index idx_c(c,d,e));");

    tk.must_get_err_code(
        "alter table t2 add constraint fk_c foreign key (c) references t1(b), \
         add constraint fk_d foreign key (d) references t1(b), \
         add constraint fk_e foreign key (e) references t1(unknown_col)",
        3734,
    );
    assert!(declared(&tk, "test", "t2").is_empty());
    assert_eq!(index_names(&tk, "test", "t2").len(), 1);

    tk.must_get_err_code(
        "alter table t2 drop index idx_c, add constraint fk_c foreign key (c) references t1(b)",
        1553,
    );
    assert!(index_names(&tk, "test", "t2").iter().any(|name| name == "idx_c"));
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
#[test]
fn partial_index_safety_rules_match_go() {
    // Go: an unsafe child predicate does not cover the FK, so TiDB creates a
    // second support index and allows the unsafe index to be dropped.
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key)");
    tk.must_exec("create table t2 (id int key, b int, c int, index idx_b(b) where c is not null)");
    tk.must_exec("alter table t2 add constraint fk_b foreign key (b) references t1(id)");
    assert_eq!(index_names(&tk, "test", "t2"), ["idx_b", "fk_b"]);
    tk.must_exec("insert into t1 values (1)");
    tk.must_exec("insert into t2 values (1, 1, null)");
    tk.must_get_err_code("delete from t1 where id = 1", 1451);
    tk.must_exec("alter table t2 drop index idx_b");
    tk.must_get_err_code("alter table t2 drop index fk_b", 1553);

    // Go: IS NOT NULL on the child key itself is safe and suppresses the
    // auto-created FK index.
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key)");
    tk.must_exec("create table t2 (id int key, b int, index idx_b(b) where b is not null)");
    tk.must_exec("alter table t2 add constraint fk_b foreign key (b) references t1(id)");
    assert_eq!(index_names(&tk, "test", "t2").len(), 1);
    tk.must_exec("insert into t1 values (1)");
    tk.must_exec("insert into t2 values (1, 1)");
    tk.must_get_err_code("delete from t1 where id = 1", 1451);

    // Go: IS NOT NULL on the referenced key itself is safe for parent-side
    // lookups and rejects an orphan child row with 1452.
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, a int, index idx_a(a) where a is not null)");
    tk.must_exec("create table t2 (id int key, b int, index idx_b(b))");
    tk.must_exec("alter table t2 add constraint fk_b foreign key (b) references t1(a)");
    assert_eq!(index_names(&tk, "test", "t2").len(), 1);
    tk.must_exec("insert into t1 values (1, 10)");
    tk.must_exec("insert into t2 values (1, 10)");
    tk.must_get_err_code("insert into t2 values (2, 20)", 1452);

    // The standalone CREATE INDEX path checks the same predicate: on a mock
    // store, which has no fast-reorg (ingest) backfill, Go refuses the
    // partial index (`index.go:1363`, captured with `gorun`), and it rejects
    // a partial key on a partitioned table before that.
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int)");
    let error = tk.must_get_err_code("create index idx_b on t (b) where b is not null", 8200);
    assert!(
        error
            .error_string()
            .contains("add partial index without fast reorg is not supported"),
        "{}",
        error.error_string()
    );
    assert!(index_names(&tk, "test", "t").is_empty());
    tk.must_exec("create table tp (a int) partition by hash(a) partitions 2");
    let error = tk.must_get_err_code("create index idx_a on tp (a) where a is not null", 8200);
    assert!(
        error
            .error_string()
            .contains("partial index on partitioned table is not supported"),
        "{}",
        error.error_string()
    );
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
// The RENAME COLUMN legs of the same Go test assert the same maintenance as
// CHANGE COLUMN, but use the metadata-only action. Self-reference setup is
// accepted with checks ON, matching Go's create-time validation.
#[test]
fn change_column_renames_follow_the_constraint_meta() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))");
    tk.must_exec("alter table t1 change id kid int");
    let keys = declared(&tk, "test", "t1");
    assert_eq!(keys.len(), 1);
    // Go: RefCols and the parent's referred Cols both follow the rename.
    assert_eq!(keys[0].ref_cols, vec!["kid".to_owned()], "Go: RefCols follow the rename");
    let referred = referred_foreign_keys(&tk, "test", "t1");
    assert_eq!(referred.len(), 1);
    assert_eq!(referred[0].2.ref_cols, vec!["kid".to_owned()], "Go: ReferredFKInfo.Cols follow the rename");

    tk.must_exec("drop table t1");
    tk.must_exec("create table t1 (id int key, b int, index(b))");
    tk.must_exec("create table t2 (a int, b int, foreign key fk(a) references t1(b))");
    tk.must_exec("alter table t2 change a aa int");
    let referred = referred_foreign_keys(&tk, "test", "t1");
    assert_eq!(referred.len(), 1);
    // Go: the parent's referred Cols are the REFERENCED columns ('b'), and
    // the child's declared Cols follow the rename to 'aa'.
    assert_eq!(referred[0].2.ref_cols, vec!["b".to_owned()], "Go: parent's referred Cols stay 'b'");
    let keys = declared(&tk, "test", "t2");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].cols, vec!["aa".to_owned()], "Go: child's Cols follow the rename");
    assert_eq!(keys[0].ref_cols, vec!["b".to_owned()]);
}

// The RENAME COLUMN legs of Go's TestRenameColumnWithForeignKeyMetaInfo
// (pkg/ddl/tests/fk/foreign_key_test.go:993, :1029, :1059, :1092):
// `alter table t1 rename column a to aa` (on a self-referencing table),
// `alter table t2 rename column a to aa` (on a child), and the two-column
// finale where both constraints follow `rename b to bb`.
#[test]
fn rename_column_follows_the_constraint_meta() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))");
    tk.must_exec("alter table t1 rename column id to kid");
    tk.must_exec("alter table t1 rename column a to aa");
    let keys = declared(&tk, "test", "t1");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].cols, vec!["aa".to_owned()]);
    assert_eq!(keys[0].ref_cols, vec!["kid".to_owned()]);
    let referred = referred_foreign_keys(&tk, "test", "t1");
    assert_eq!(referred.len(), 1);
    assert_eq!(referred[0].2.ref_cols, vec!["kid".to_owned()]);

    tk.must_exec("drop table t1");
    tk.must_exec("create table t1 (id int key, b int, index(b))");
    tk.must_exec(
        "create table t2 (a int, b int, foreign key fk_1(a) references t1(b), \
         foreign key fk_2(b) references t1(b))",
    );
    tk.must_exec("alter table t2 rename column a to aa");
    tk.must_exec("alter table t1 rename column b to bb");
    tk.must_exec("alter table t2 rename column b to bb");
    let keys = declared(&tk, "test", "t2");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].cols, vec!["aa".to_owned()]);
    assert_eq!(keys[0].ref_cols, vec!["bb".to_owned()]);
    assert_eq!(keys[1].cols, vec!["bb".to_owned()]);
    assert_eq!(keys[1].ref_cols, vec!["bb".to_owned()]);
    let referred = referred_foreign_keys(&tk, "test", "t1");
    assert_eq!(referred.len(), 2);
    assert!(referred.iter().all(|(_, _, fk)| fk.ref_cols == ["bb"]));
}

// Go's fourth pass drop (`pkg/ddl/tests/fk/foreign_key_test.go:920`, pass
// shape 1): `alter table t2 drop index idxb` where `idxb (b)` is the
// constraint's cover and `b` is ALSO the child's clustered primary handle —
// Go ALLOWS it because the handle keeps answering the constraint's lookups
// (Go `checkIndexNeededInForeignKey`'s PKIsHandle escape applies to the
// declared-FK branch too, pkg/ddl/foreign_key.go:459-464 via :476).
#[test]
fn dropping_the_child_handle_cover_is_allowed() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table t1 (id int key, b int, index idxb (b))");
    tk.must_exec(
        "create table t2 (a int, b int key, index idxa (a), index idxb (b), \
         foreign key fk_b(b) references t1(id))",
    );
    // Go: the clustered handle still covers the child constraint.
    tk.must_exec("alter table t2 drop index idxb");
    assert!(index_names(&tk, "test", "t2")
        .iter()
        .all(|name| !name.eq_ignore_ascii_case("idxb")));
    assert_eq!(declared(&tk, "test", "t2").len(), 1);
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
// The table-lifecycle owner now rewrites every affected `ref_schema`/
// `ref_table` before moving the catalog entry, including a self-reference on
// the moved table and children in another schema.
#[test]
fn rename_table_rewrites_the_constraint_reference() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test2");
    tk.must_exec("create database test3");
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))");
    tk.must_exec("rename table test.t1 to test2.t2");
    let keys = declared(&tk, "test2", "t2");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].ref_schema, "test2");
    assert_eq!(keys[0].ref_table, "t2");
    assert_eq!(referred_foreign_keys(&tk, "test2", "t2").len(), 1);

    tk.must_exec("drop table test2.t2");
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int key, a int)");
    tk.must_exec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))");
    tk.must_exec("use test2");
    tk.must_exec("rename table test.t2 to test2.tt2");
    tk.must_exec("alter table test2.tt2 rename to test2.tt3");
    let keys = declared(&tk, "test2", "tt3");
    assert_eq!(keys[0].ref_schema, "test");
    assert_eq!(keys[0].ref_table, "t1");
    tk.must_exec("rename table test.t1 to test3.tt1");
    let keys = declared(&tk, "test2", "tt3");
    assert_eq!(keys[0].ref_schema, "test3");
    assert_eq!(keys[0].ref_table, "tt1");
    assert_eq!(referred_foreign_keys(&tk, "test3", "tt1").len(), 1);
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
#[test]
fn truncate_or_drop_of_a_referenced_table_reports_go_errnos() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1 (id int key, b int, index(b))");
    tk.must_exec("create table t2 (a int, b int, foreign key fk_b(b) references t1(b))");

    tk.must_exec("set @@foreign_key_checks=1");
    let error = tk.must_get_err_code("truncate table t1", 1701);
    assert_eq!(
        error.message,
        "Cannot truncate a table referenced in a foreign key constraint (`test`.`t2` CONSTRAINT `fk_b`)"
    );

    let error = tk.must_get_err_code("drop table t1", 3730);
    assert_eq!(
        error.message,
        "Cannot drop table 't1' referenced by a foreign key constraint 'fk_b' on table 't2'."
    );

    tk.must_exec("set @@foreign_key_checks=0");
    tk.must_exec("truncate table t1");
    tk.must_exec("drop table t1");
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
#[test]
fn drop_index_needed_by_a_foreign_key_is_refused_regardless_of_checks() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");

    // Go case 1: single-column covers on both sides.
    tk.must_exec("create table t1 (id int key, b int, index idx (b))");
    tk.must_exec("create table t2 (a int, b int, index idx (b), foreign key fk_b(b) references t1(b))");
    for drop in ["alter table t1 drop index idx", "alter table t2 drop index idx"] {
        // Even with the checks disabled, the index the constraint uses
        // cannot be dropped.
        for checks in [0, 1] {
            tk.must_exec(&format!("set @@foreign_key_checks={checks}"));
            let error = tk.must_get_err_code(drop, 1553);
            assert_eq!(
                error.message,
                "Cannot drop index 'idx': needed in a foreign key constraint",
                "{drop} with checks={checks}"
            );
        }
    }

    // Go case 2: composite covers (t1's idx (id, b), t2's idx (b, a)) —
    // still the constraint's last cover on each side.
    tk.must_exec("drop table if exists t2, t1");
    tk.must_exec("create table t1 (id int, b int, index idx (id, b))");
    tk.must_exec("create table t2 (a int, b int, index idx (b, a), foreign key fk_b(b) references t1(id))");
    for drop in ["alter table t1 drop index idx", "alter table t2 drop index idx"] {
        tk.must_get_err_code(drop, 1553);
    }

    // Go's pass shape 1: the parent's cover is replacable by its primary
    // key, and the child's non-needed index drops freely. The FOURTH drop of
    // Go's shape — `alter table t2 drop index idxb`, the constraint's own
    // cover whose column IS the child's clustered primary handle — is covered
    // by the focused regression above.
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("drop table if exists t2, t1");
    tk.must_exec("create table t1 (id int key, b int, index idxb (b))");
    tk.must_exec(
        "create table t2 (a int, b int key, index idxa (a), index idxb (b), \
         foreign key fk_b(b) references t1(id))",
    );
    for drop in ["alter table t1 drop index idxb", "alter table t2 drop index idxa"] {
        tk.must_exec(drop);
    }

    // The second pass shape: re-adding a cover re-opens the drops.
    tk.must_exec("drop table if exists t2, t1");
    tk.must_exec("create table t1 (id int key, b int, index idxb (b), unique index idx(b, id))");
    tk.must_exec(
        "create table t2 (a int, b int key, index idx (b, a), index idxb (b), index idxab(a, b), \
         foreign key fk_b(b) references t1(b))",
    );
    for alter in [
        "alter table t1 drop index idxb",
        "alter table t1 add index idxb (b)",
        "alter table t1 drop index idx",
        "alter table t2 drop index idx",
        "alter table t2 add index idx (b, a)",
        "alter table t2 drop index idxb",
        "alter table t2 drop index idxab",
    ] {
        tk.must_exec(alter);
    }

    tk.must_exec("admin check table t2");
    tk.must_exec("delete from t2");
}
