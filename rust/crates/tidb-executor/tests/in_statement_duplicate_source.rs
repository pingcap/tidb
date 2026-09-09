//! In-statement duplicate keys and REPLACE with an AUTO_INCREMENT key: IGNORE
//! keeps the FIRST row and skips the later duplicate while later rows still
//! insert; a plain insert fails; REPLACE on an auto-inc key reports 2
//! affected rows (delete + insert).

use tidb_executor::{
    ddl, run_create_table_in, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn table(sql: &str, catalog: &mut Catalog) {
    ddl::run_create_table_in(
        sql,
        catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
}

#[test]
fn ignore_keeps_first_and_skips_in_statement_duplicate() {
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);
    let mut catalog = Catalog::default();
    table("create table t (a int primary key, v int)", &mut catalog);

    let inserted = run_insert_on(
        "insert ignore into t values (1, 10), (1, 11), (2, 20)",
        &mut catalog,
        &ignore,
    )
    .unwrap();
    assert_eq!(inserted, 2, "the duplicate is skipped, the rest insert");
    let rows = run_select_on("select a, v from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1), Int(10)], [Int(2), Int(20)]]");
}

#[test]
fn plain_insert_fails_on_in_statement_duplicate() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    table("create table t (a int primary key, v int)", &mut catalog);
    let error = run_insert_on("insert into t values (1, 10), (1, 11)", &mut catalog, &strict)
        .expect_err("the in-statement duplicate must fail");
    assert!(error.to_string().contains("Duplicate entry"), "{error}");
}

#[test]
fn replace_on_auto_inc_key_counts_two() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    table("create table u (id int auto_increment primary key, v int)", &mut catalog);
    run_insert_on("insert into u (v) values (1)", &mut catalog, &strict).unwrap();
    let affected = run_insert_on("replace into u values (1, 100)", &mut catalog, &strict).unwrap();
    assert_eq!(affected, 2, "delete + insert");
    let rows = run_select_on("select id, v from u", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1), Int(100)]]");
}
