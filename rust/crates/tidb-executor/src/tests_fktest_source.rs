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

//! Ports of Go `pkg/executor/test/fktest/foreign_key_test.go`: foreign-key
//! enforcement on child INSERT/UPDATE and parent UPDATE/DELETE, at the
//! single-statement boundary this tier owns
//! (`crate::foreign_key::check_child_rows` / `cascade_parent_changes`).
//!
//! SCOPE NOTE. Go drives the cases through `foreignKeyTestCase1` (8 index
//! shapes × optimistic/pessimistic transactions across two sessions). The
//! transaction/lock arms are the `foreign_key_check_and_lock` gap below;
//! the statement-local contracts — 1452 on orphan inserts, NULL child keys
//! passing, 1451 on parent updates/deletes that orphan referenced keys —
//! are running tests with Go's exact expectations.

use crate::{
    run_create_table_on, run_delete_on, run_insert_on, run_select_on, run_update_on, Catalog,
    StmtContext,
};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("create {sql:?}: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?}: {error:?}"));
}

fn expect_insert_error(catalog: &mut Catalog, sql: &str, code: u16) -> crate::DriverError {
    let error = run_insert_on(sql, catalog, &ctx()).expect_err(sql);
    assert_eq!(error.clone().to_mysql_error().code, code, "{error:?}");
    error
}

fn expect_update_error(catalog: &mut Catalog, sql: &str, code: u16) {
    let error = run_update_on(sql, catalog, &ctx()).expect_err(sql);
    assert_eq!(error.clone().to_mysql_error().code, code, "{error:?}");
}

fn render(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => format!("{value}"),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
}

fn rows_text(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("select {sql:?}: {error:?}"))
        .iter()
        .map(|row| row.iter().map(render).collect())
        .collect()
}

/// Go `foreign_key_test.go:115::TestForeignKeyOnInsertChildTable` (cases 1–4
/// and the primary-key-handle twins, cases 10/11): orphan child inserts fail
/// 1452 (`ErrNoReferencedRow2`) on every index shape, NULL child keys pass,
/// INSERT … SELECT enforces per row, and a child column falling back to its
/// DEFAULT participates in the check.
#[test]
fn foreign_key_on_insert_child_table() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t_data (id int, a int, b int)");
    insert(&mut catalog, "insert into t_data (id, a, b) values (1, 1, 1), (2, 2, 2)");

    // foreignKeyTestCase1 cases 1-4: unique/non-unique indexes over exactly
    // the FK columns and over FK+extra columns. (Cases 5-8 additionally
    // toggle @@tidb_enable_clustered_index, a session variable with no
    // surface here; the PRIMARY KEY FK shape itself is exercised by the
    // case-10 block below.)
    let prepare: [&[&str]; 4] = [
        &[
            "create table t1 (id int, a int, b int, unique index(id), unique index(a, b))",
            "create table t2 (b int, name varchar(10), a int, id int, unique index(id), unique index (a,b), foreign key fk(a, b) references t1(a, b))",
        ],
        &[
            "create table t1 (id int key, a int, b int, unique index(id), unique index(a, b, id))",
            "create table t2 (b int, a int, id int key, name varchar(10), unique index (a,b, id), foreign key fk(a, b) references t1(a, b))",
        ],
        &[
            "create table t1 (id int key, a int, b int, unique index(id), index(a, b))",
            "create table t2 (b int, a int, name varchar(10), id int key, index (a, b), foreign key fk(a, b) references t1(a, b))",
        ],
        &[
            "create table t1 (id int key, a int, b int, unique index(id), index(a, b, id))",
            "create table t2 (name varchar(10), b int, a int, id int key, index (a, b, id), foreign key fk(a, b) references t1(a, b))",
        ],
    ];
    for statements in prepare {
        for sql in statements {
            create(&mut catalog, sql);
        }
        insert(&mut catalog, "insert into t1 (id, a, b) values (1, 1, 1)");
        insert(&mut catalog, "insert into t2 (id, a, b) values (1, 1, 1)");
        // NULL FK components pass the check in Go for every non-notNull case.
        insert(&mut catalog, "insert into t2 (id, a, b) values (2, null, 1)");
        insert(&mut catalog, "insert into t2 (id, a, b) values (3, 1, null)");
        insert(&mut catalog, "insert into t2 (id, a, b) values (4, null, null)");
        // Orphans on either component fail 1452.
        expect_insert_error(&mut catalog, "insert into t2 (id, a, b) values (5, 1, 0)", 1452);
        expect_insert_error(&mut catalog, "insert into t2 (id, a, b) values (6, 0, 1)", 1452);
        expect_insert_error(&mut catalog, "insert into t2 (id, a, b) values (7, 2, 2)", 1452);
        // INSERT ... SELECT enforces per source row.
        run_delete_on("delete from t2", &mut catalog, &ctx()).expect("delete");
        insert(
            &mut catalog,
            "insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=1",
        );
        expect_insert_error(
            &mut catalog,
            "insert into t2 (id, a, b) select id, a, b from t_data where t_data.id=2",
            1452,
        );
        for name in ["t2", "t1"] {
            crate::run_drop_table_in(
                &format!("drop table {name}"),
                &mut catalog,
                "test",
                tidb_parser::SqlMode::default(),
                true,
            )
            .unwrap_or_else(|e| panic!("drop {name}: {e:?}"));
        }
    }

    // Case-10: the FK column is covered by the integer handle PK and HAS a
    // default — `insert into t2 (id) values (10)` fills a=0 and fails 1452.
    create(&mut catalog, "create table t1 (id int,a int, primary key(id))");
    create(
        &mut catalog,
        "create table t2 (id int key,a int not null default 0, index (a), foreign key fk(a) references t1(id))",
    );
    insert(&mut catalog, "insert into t1 values (1, 1)");
    insert(&mut catalog, "insert into t2 values (1, 1)");
    expect_insert_error(&mut catalog, "insert into t2 (id) values (10)", 1452);
    expect_insert_error(&mut catalog, "insert into t2 values (3, 2)", 1452);
    // Go keeps case-10's parent for case-11; only the child is replaced.
    crate::run_drop_table_in(
        "drop table t2",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap_or_else(|e| panic!("drop t2: {e:?}"));

    // Case-11: without the default the same insert fills NULL and passes.
    create(
        &mut catalog,
        "create table t2 (id int key,a int, index (a), foreign key fk(a) references t1(id))",
    );
    insert(&mut catalog, "insert into t2 values (1, 1)");
    insert(&mut catalog, "insert into t2 (id) values (10)");
    expect_insert_error(&mut catalog, "insert into t2 values (3, 2)", 1452);
    assert_eq!(
        rows_text(&catalog, "select id, a from t2 order by id"),
        vec![vec!["1", "1"], vec!["10", "<nil>"]],
    );
}

/// Go `foreign_key_test.go:178::TestForeignKeyOnInsertDuplicateUpdateChildTable`:
/// Go checks the FINAL (post-ODKU-assignment) child values against the
/// parent — `on duplicate key update a = 100` fails 1452 while `update a =
/// 12, b = 22` succeeds even when the would-be-inserted values are missing.
/// Measured this session the check here runs against the INSERT values
/// instead: `update a = 100` is allowed (orphan state), `update a = 12,
/// b = 22` errors 1452 when the inserted values (14, 26) are missing — the
/// reverse of Go on both arms.
#[test]
#[ignore = "go-parity-gap: ODKU child FK check inspects insert values, not the updated values (both arm directions diverge)"]
fn foreign_key_on_insert_duplicate_update_child_table() {}

/// The NULL-assignment arm of Go
/// `foreign_key_test.go:178::TestForeignKeyOnInsertDuplicateUpdateChildTable`:
/// Go executes `on duplicate key update a = null` (NULL child keys never
/// reference anything). Measured this session: the same statement fails here
/// with `ForeignKeyNoReferencedRow` — the ODKU write path checks the new
/// values without the NULL exemption its insert path has.
#[test]
#[ignore = "go-parity-gap: ODKU NULL child-key assignment errors 1452 instead of passing"]
fn foreign_key_odku_null_child_key_passes() {}

/// The transaction arms of Go
/// `foreign_key_test.go:178::TestForeignKeyOnInsertDuplicateUpdateChildTable`
/// (`begin`/`rollback` visibility and in-txn parent deletion).
#[test]
#[ignore = "go-parity-gap: explicit transactions unported"]
fn foreign_key_odku_in_txn() {}

/// Go `foreign_key_test.go:279::TestForeignKeyCheckAndLock`: two sessions'
/// optimistic/pessimistic transactions interleave child inserts with parent
/// updates/deletes; conflicts surface as `Write conflict` and the
/// pessimistic arms as `[planner:1451]Cannot delete or update a parent row …`.
///
/// go-parity-gap: needs multi-session transactions, lock records and
/// fair-locking variables.
#[test]
#[ignore = "go-parity-gap: multi-session txn locking (Write conflict / pessimistic 1451) unported"]
fn foreign_key_check_and_lock() {}

/// Go `foreign_key_test.go:501::TestForeignKeyOnInsertOnDuplicateParentTableCheck`
/// (notNull primary-key arms + case-10): parent-side ODKU updates that would
/// orphan a referenced key fail 1451 (`ErrRowIsReferenced2`), plain parent
/// updates/deletes of referenced keys fail 1451, and id-repointing ODKU
/// moves rows.
#[test]
fn foreign_key_on_insert_on_duplicate_parent_table_check() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t1 (id int, a int, b int, unique index(id), unique index(a, b))");
    create(
        &mut catalog,
        "create table t2 (b int, name varchar(10), a int, id int, unique index(id), unique index (a,b), foreign key fk(a, b) references t1(a, b))",
    );
    insert(&mut catalog, "insert into t1 (id, a, b) values (1, 11, 21),(2, 12, 22), (3, 13, 23), (4, 14, 24)");
    insert(&mut catalog, "insert into t2 (id, a, b, name) values (1, 11, 21, 'a')");

    // Parent ODKU rewrites that keep the referenced (a, b) succeed, exactly
    // as in Go's two-statement sequence: 12 -> 112 -> 1112, 13 -> 1013.
    insert(
        &mut catalog,
        "insert into t1 (id, a, b) values (2, 12, 22) on duplicate key update a=a+100, b=b+200",
    );
    insert(
        &mut catalog,
        "insert into t1 (id, a, b) values (3, 13, 23), (2, 12, 22) on duplicate key update a=a+1000, b=b+2000",
    );
    // Re-pointing the parent's id (the referenced (a, b) stays) succeeds.
    insert(
        &mut catalog,
        "insert into t1 (id, a, b) values (1, 11, 21) on duplicate key update id=11",
    );
    assert_eq!(
        rows_text(&catalog, "select id, a, b from t1 order by id"),
        vec![
            vec!["2", "1112", "2222"],
            vec!["3", "1013", "2023"],
            vec!["4", "14", "24"],
            vec!["11", "11", "21"],
        ],
    );
    assert_eq!(
        rows_text(&catalog, "select id, a, b, name from t2 order by id"),
        vec![vec!["1", "11", "21", "a"]],
    );

    // Rewriting the referenced (a, b) of a parent row fails 1451 (plain
    // UPDATE form; Go drives the same assertion through the ODKU form, which
    // is the arm gap below — this tier's ODKU path skips the parent check).
    expect_update_error(&mut catalog, "update t1 set a=a+10, b=b+20 where id = 11", 1451);

    // Parent DELETE of a referenced key fails 1451 (Go's pessimistic arms).
    let error = run_delete_on("delete from t1 where id = 11", &mut catalog, &ctx())
        .expect_err("referenced parent row");
    assert_eq!(error.clone().to_mysql_error().code, 1451, "{error:?}");
    assert_eq!(
        error.to_mysql_error().message,
        "Cannot delete or update a parent row: a foreign key constraint fails (`test`.`t2`, CONSTRAINT `fk` FOREIGN KEY (`a`, `b`) REFERENCES `t1` (`a`, `b`))",
    );
}

/// The `insert into t1 (id, a, b) values (11, 11, 21) on duplicate key
/// update a=a+10, b=b+20` arm of Go
/// `foreign_key_test.go:501::TestForeignKeyOnInsertOnDuplicateParentTableCheck`
/// (the referenced key is rewritten through ODKU — Go fails it with
/// `ErrRowIsReferenced2`, 1451) and the case-10 tail, which runs under
/// `set @@foreign_key_checks=0`, a session toggle with no statement surface
/// here (the toggle itself is pinned by `StmtContext::with_foreign_key_checks`).
/// Measured this session: the parent-side ODKU path skips the child-reference
/// check entirely, so the 1451 arm cannot be pinned.
#[test]
#[ignore = "go-parity-gap: parent-side ODKU skips the 1451 check; @@foreign_key_checks=0 has no SQL surface"]
fn foreign_key_parent_odku_and_checks_off_arms() {}

/// Go `foreign_key_test.go:570::TestForeignKeyConcurrentInsertChildTable`:
/// ten goroutines insert 20 valid child rows each (`a` = `cnt%4+1`, all
/// present in `t1`); no statement may fail. The inserts here run on one
/// connection (the catalog is not shared across threads), which pins the
/// per-row FK verdicts under Go's data.
#[test]
fn foreign_key_concurrent_insert_child_table_all_rows_valid() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table t1 (id int, a int, primary key (id))");
    create(
        &mut catalog,
        "create table t2 (id int, a int, index(a), foreign key fk(a) references t1(id))",
    );
    insert(&mut catalog, "insert into t1 (id, a) values (1, 11),(2, 12), (3, 13), (4, 14)");
    for worker in 0..10 {
        for cnt in 0..20 {
            let id = cnt % 4 + 1;
            insert(
                &mut catalog,
                &format!("insert into t2 (id, a) values ({}, {})", worker * 20 + cnt, id),
            );
        }
    }
    let rows = run_select_on("select count(*) from t2", &catalog, &ctx()).expect("count");
    assert_eq!(render(&rows[0][0]), "200", "every concurrent insert landed");
}
