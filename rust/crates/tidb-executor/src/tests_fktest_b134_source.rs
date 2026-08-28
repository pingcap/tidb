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

//! Ports of `pkg/executor/test/fktest/foreign_key_test.go` items 901–923.
//!
//! The running tests use the statement-local catalog/DML boundary already
//! owned by `tidb-executor`. The remaining tests are explicit gaps for
//! session transactions, privilege checks, failpoints, runtime-plan output,
//! and lock scheduling; they are not approximated with a different API.

use crate::{
    run_alter_table_in, run_create_table_on, run_delete_on, run_insert_on, run_select_on,
    run_update_on,
};
use crate::{Catalog, DriverError, StmtContext};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn expect_error_code(error: DriverError, code: u16) {
    assert_eq!(
        error.clone().to_mysql_error().code,
        code,
        "unexpected MySQL error: {error:?}"
    );
}

fn expect_update_error(catalog: &mut Catalog, sql: &str, code: u16) {
    expect_error_code(run_update_on(sql, catalog, &ctx()).expect_err(sql), code);
}

fn expect_delete_error(catalog: &mut Catalog, sql: &str, code: u16) {
    expect_error_code(run_delete_on(sql, catalog, &ctx()).expect_err(sql), code);
}

fn render(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => value.to_string(),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    }
}

fn rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("select {sql:?} failed: {error:?}"))
        .iter()
        .map(|row| row.iter().map(render).collect())
        .collect()
}

/// Go `TestForeignKeyOnUpdateChildTable` (:598): an UPDATE on the child is
/// checked against the parent, while a NULL in any MATCH SIMPLE component is
/// exempt. The same 1452 contract is exercised for composite keys here.
#[test]
fn foreign_key_on_update_child_table() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table p (id int primary key, a int, b int, unique key ab(a,b))",
    );
    create(
        &mut catalog,
        "create table c (id int primary key, a int, b int, name varchar(10), foreign key fk(a,b) references p(a,b))",
    );
    insert(
        &mut catalog,
        "insert into p values (1,11,21),(2,12,22),(3,13,23)",
    );
    insert(&mut catalog, "insert into c values (1,11,21,'a')");

    expect_update_error(&mut catalog, "update c set a=100,b=200 where id=1", 1452);
    expect_update_error(&mut catalog, "update c set a=12,b=23 where id=1", 1452);
    run_update_on("update c set a=12,b=22 where id=1", &mut catalog, &ctx())
        .expect("existing composite parent key is accepted");
    run_update_on("update c set a=null,b=22 where id=1", &mut catalog, &ctx())
        .expect("NULL exempts the composite child key");
    run_update_on("update c set b=null where id=1", &mut catalog, &ctx())
        .expect("a second NULL component remains exempt");
    run_update_on("update c set a=13,b=23 where id=1", &mut catalog, &ctx())
        .expect("the child can be restored to an existing parent key");
    assert_eq!(
        rows(&catalog, "select id,a,b,name from c"),
        vec![vec![
            "1".to_owned(),
            "13".to_owned(),
            "23".to_owned(),
            "a".to_owned()
        ]],
    );
}

/// Go `TestForeignKeyOnUpdateParentTableCheck` (:694): changing a referenced
/// parent key is restricted, but changing only an unreferenced column is not.
#[test]
fn foreign_key_on_update_parent_table_check() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table p (id int primary key, a int unique, note int)",
    );
    create(
        &mut catalog,
        "create table c (id int primary key, pid int, foreign key fk(pid) references p(a))",
    );
    insert(&mut catalog, "insert into p values (1,11,21),(2,12,22)");
    insert(&mut catalog, "insert into c values (1,11)");

    run_update_on("update p set note=99 where id=1", &mut catalog, &ctx())
        .expect("unreferenced parent columns do not trigger FK action");
    expect_update_error(&mut catalog, "update p set a=111 where id=1", 1451);
    assert_eq!(
        rows(&catalog, "select id,a,note from p order by id"),
        vec![
            vec!["1".to_owned(), "11".to_owned(), "99".to_owned()],
            vec!["2".to_owned(), "12".to_owned(), "22".to_owned()],
        ],
    );
}

/// Go `TestForeignKeyOnDeleteParentTableCheck` (:745): a referenced parent
/// row cannot be deleted under the default restricting action.
#[test]
fn foreign_key_on_delete_parent_table_check() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table p (id int primary key, a int unique)",
    );
    create(
        &mut catalog,
        "create table c (id int primary key, pid int, foreign key fk(pid) references p(a))",
    );
    insert(&mut catalog, "insert into p values (1,11),(2,12),(3,13)");
    insert(&mut catalog, "insert into c values (1,11)");

    run_delete_on("delete from p where id=2", &mut catalog, &ctx())
        .expect("an unreferenced parent row can be deleted");
    expect_delete_error(&mut catalog, "delete from p where id=1", 1451);
    assert_eq!(
        rows(&catalog, "select id from p order by id"),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );
}

/// Go `TestForeignKeyOnDeleteCascade` (:807): parent deletion removes direct
/// dependents, including rows found through an ordinary child index.
#[test]
fn foreign_key_on_delete_cascade() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table p (id int primary key)");
    create(
        &mut catalog,
        "create table c (id int primary key, pid int, index pid_idx(pid), foreign key fk(pid) references p(id) on delete cascade)",
    );
    insert(&mut catalog, "insert into p values (1),(2),(3)");
    insert(&mut catalog, "insert into c values (10,1),(11,1),(12,2)");

    run_delete_on("delete from p where id=1", &mut catalog, &ctx()).expect("cascade delete");
    assert_eq!(
        rows(&catalog, "select id,pid from c order by id"),
        vec![vec!["12".to_owned(), "2".to_owned()]],
    );
}

/// Go `TestForeignKeyOnDeleteCascade2` (:1017): self-referential cascades
/// recurse through the dependent chain.
#[test]
fn foreign_key_on_delete_cascade2() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (id int primary key, pid int, index pid_idx(pid))",
    );
    run_alter_table_in(
        "alter table t add foreign key fk(pid) references t(id) on delete cascade",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect("add self-referential cascade foreign key");
    insert(&mut catalog, "insert into t values (1,null)");
    insert(&mut catalog, "insert into t values (2,1)");
    insert(&mut catalog, "insert into t values (3,2)");
    insert(&mut catalog, "insert into t values (4,3)");

    run_delete_on("delete from t where id=1", &mut catalog, &ctx()).expect("transitive cascade");
    assert!(rows(&catalog, "select id,pid from t").is_empty());
}

/// Go `TestForeignKeyGenerateCascadeAST` (:1224) compares restored AST text
/// for generated cascade DELETE/UPDATE statements. The Rust cascade engine
/// applies row changes directly and has no public AST generator to compare.
#[test]
#[ignore = "go-parity-gap: GenCascadeDeleteAST/GenCascadeSetNullAST/GenCascadeUpdateAST are not exposed in tidb-executor"]
fn foreign_key_generate_cascade_ast() {}

/// Go `TestForeignKeyOnDeleteSetNull` (:1276): deleting a parent nulls the
/// child key instead of deleting the child row.
#[test]
fn foreign_key_on_delete_set_null() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table p (id int primary key)");
    create(
        &mut catalog,
        "create table c (id int primary key, pid int, index pid_idx(pid), foreign key fk(pid) references p(id) on delete set null)",
    );
    insert(&mut catalog, "insert into p values (1),(2)");
    insert(&mut catalog, "insert into c values (10,1),(11,2),(12,null)");

    run_delete_on("delete from p where id=1", &mut catalog, &ctx()).expect("set null");
    assert_eq!(
        rows(&catalog, "select id,pid from c order by id"),
        vec![
            vec!["10".to_owned(), "<nil>".to_owned()],
            vec!["11".to_owned(), "2".to_owned()],
            vec!["12".to_owned(), "<nil>".to_owned()],
        ],
    );
}

/// Go `TestForeignKeyOnDeleteSetNull2` (:1400): the SET NULL action also
/// works for a self-referential table and leaves deeper rows in place.
#[test]
fn foreign_key_on_delete_set_null2() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (id int primary key, pid int, index pid_idx(pid))",
    );
    run_alter_table_in(
        "alter table t add foreign key fk(pid) references t(id) on delete set null",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect("add self-referential set-null foreign key");
    insert(&mut catalog, "insert into t values (1,null)");
    insert(&mut catalog, "insert into t values (2,1)");
    insert(&mut catalog, "insert into t values (3,2)");

    run_delete_on("delete from t where id=1", &mut catalog, &ctx()).expect("set null chain");
    assert_eq!(
        rows(&catalog, "select id,pid from t order by id"),
        vec![
            vec!["2".to_owned(), "<nil>".to_owned()],
            vec!["3".to_owned(), "2".to_owned()],
        ],
    );
}

/// Go `TestForeignKeyOnUpdateCascade` (:1605): an UPDATE of the parent key
/// propagates to the child key, while NULL child keys remain unaffected.
#[test]
fn foreign_key_on_update_cascade() {
    let mut catalog = Catalog::default();
    create(&mut catalog, "create table p (id int primary key)");
    create(
        &mut catalog,
        "create table c (id int primary key, pid int, index pid_idx(pid), foreign key fk(pid) references p(id) on update cascade)",
    );
    insert(&mut catalog, "insert into p values (1),(2)");
    insert(&mut catalog, "insert into c values (10,1),(11,2),(12,null)");

    run_update_on("update p set id=10 where id=1", &mut catalog, &ctx()).expect("update cascade");
    assert_eq!(
        rows(&catalog, "select id,pid from c order by id"),
        vec![
            vec!["10".to_owned(), "10".to_owned()],
            vec!["11".to_owned(), "2".to_owned()],
            vec!["12".to_owned(), "<nil>".to_owned()],
        ],
    );
}

/// Go `TestForeignKeyOnUpdateCascade2` (:1842): a self-referential update
/// cascades through a changed parent id without touching unrelated rows.
#[test]
fn foreign_key_on_update_cascade2() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (id int primary key, pid int, index pid_idx(pid))",
    );
    run_alter_table_in(
        "alter table t add foreign key fk(pid) references t(id) on update cascade",
        &mut catalog,
        "test",
        &ctx(),
    )
    .expect("add self-referential update-cascade foreign key");
    insert(&mut catalog, "insert into t values (1,null)");
    insert(&mut catalog, "insert into t values (2,1)");
    insert(&mut catalog, "insert into t values (3,2)");

    run_update_on("update t set id=10 where id=1", &mut catalog, &ctx())
        .expect("self-referential update cascade");
    assert_eq!(
        rows(&catalog, "select id,pid from t order by id"),
        vec![
            vec!["2".to_owned(), "10".to_owned()],
            vec!["3".to_owned(), "2".to_owned()],
            vec!["10".to_owned(), "<nil>".to_owned()],
        ],
    );
}

/// Go `TestDMLExplainAnalyzeFKInfo` (:1918) checks FK runtime-plan nodes and
/// timing fields. This tier has no EXPLAIN ANALYZE runtime-stat tree.
#[test]
#[ignore = "go-parity-gap: DML EXPLAIN ANALYZE FK runtime-plan nodes are unported"]
fn dml_explain_analyze_fk_info() {}

/// Go `TestForeignKeyOnInsertOnDuplicateUpdate` (:1949) exercises parent and
/// child FK checks through INSERT ... ON DUPLICATE KEY UPDATE, including
/// transaction rollback. The current ODKU path is already recorded as a
/// separate FK gap and is not restated with weaker assertions here.
#[test]
#[ignore = "go-parity-gap: ODKU FK check ordering and transaction rollback are not parity-complete"]
fn foreign_key_on_insert_on_duplicate_update() {}

/// Go `TestExplainAnalyzeDMLWithFKInfo` (:1994) pins the full FK operator tree
/// for INSERT/UPDATE/DELETE. No equivalent plan renderer exists in this crate.
#[test]
#[ignore = "go-parity-gap: full DML EXPLAIN ANALYZE FK operator tree is unported"]
fn explain_analyze_dml_with_fk_info() {}

/// Go `TestForeignKeyRuntimeStats` (:2327) tests Clone/Merge/String on Go's
/// FK runtime-stat structs. The Rust statement context has no equivalent
/// public FK runtime-stat value type.
#[test]
#[ignore = "go-parity-gap: FK runtime-stat structs and String formatting are unported"]
fn foreign_key_runtime_stats() {}

/// Go `TestPrivilegeCheckInForeignKeyCascade` (:2346) requires users, roles,
/// and privilege checks inside cascade sub-statements.
#[test]
#[ignore = "go-parity-gap: users/roles and cascade privilege checks are unported"]
fn privilege_check_in_foreign_key_cascade() {}

/// Go `TestForeignKeyIssue39732` (:2403) combines prepared statements,
/// statement summaries, authentication, and a cascading delete.
#[test]
#[ignore = "go-parity-gap: prepared statement/authentication/statement-summary FK path is unported"]
fn foreign_key_issue39732() {}

/// Go `TestForeignKeyOnReplaceIntoChildTable` (:2421) covers REPLACE's
/// delete-then-insert FK ordering and transaction behavior. The local DML
/// surface does not yet expose the complete REPLACE FK contract.
#[test]
#[ignore = "go-parity-gap: REPLACE child-side FK ordering and transaction rollback are incomplete"]
fn foreign_key_on_replace_into_child_table() {}

/// Go `TestForeignKeyLargeTxnErr` (:2483) requires the TiKV transaction-size
/// limiter to be exercised by a large cascade.
#[test]
#[ignore = "go-parity-gap: TiKV transaction-size limit and large cascade are unported"]
fn foreign_key_large_txn_err() {}

/// Go `TestForeignKeyAndLockView` (:2513) observes a lock-waiting transaction
/// through INFORMATION_SCHEMA.TIDB_TRX and then checks deadlock handling.
#[test]
#[ignore = "go-parity-gap: pessimistic locks, deadlocks, and TIDB_TRX are unported"]
fn foreign_key_and_lock_view() {}

/// Go `TestFKBuild` (:2543) injects an infoschema syncer failpoint while
/// renaming a referenced table, then verifies the FK remains usable.
#[test]
#[ignore = "go-parity-gap: infoschema syncer failpoint and rename-table FK rebuild are unported"]
fn fk_build() {}

/// Go `TestLockKeysInDML` (:2561) asserts that a child INSERT holds the
/// parent key until commit and delays another transaction's parent UPDATE.
#[test]
#[ignore = "go-parity-gap: multi-session transaction lock-key timing is unported"]
fn lock_keys_in_dml() {}

/// Go `TestLockKeysInInsertIgnore` (:2594) is the INSERT IGNORE variant of
/// the parent-key lock timing test, including fair-locking configuration.
#[test]
#[ignore = "go-parity-gap: INSERT IGNORE lock-key timing and fair locking are unported"]
fn lock_keys_in_insert_ignore() {}

/// Go `pkg/executor/test/fktest/main_test.go:26::TestMain` is suite
/// bootstrap/goleak setup rather than product behavior.
#[test]
#[ignore = "skipped-reason: Go suite bootstrap/goleak has no Rust test behavior"]
fn fktest_main_is_bootstrap() {}
