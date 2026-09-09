//! `INSERT ... ON DUPLICATE KEY UPDATE`: a non-duplicate key takes the insert
//! branch (1 row), a duplicate takes the UPDATE branch (2 rows, Go's
//! accounting), and `VALUES(col)` refers to the would-be-inserted value.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn non_duplicate_takes_the_insert_branch() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let affected = run_insert_on(
        "insert into t values (1, 'abc') on duplicate key update b = 'upd'",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(affected, 1, "the insert branch affects one row");
}

#[test]
fn duplicate_takes_the_update_branch() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();
    let affected = run_insert_on(
        "insert into t values (1, 'xyz') on duplicate key update b = 'upd'",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(affected, 2, "Go counts the update as 2 affected rows");
    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0].as_raw_bytes()), "Some([117, 112, 100])");
}

#[test]
fn values_alias_refers_to_the_inserted_value() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();
    run_insert_on(
        "insert into t values (1, 'qrs') on duplicate key update b = values(b)",
        &mut catalog,
        &strict,
    )
    .unwrap();
    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0].as_raw_bytes()), "Some([113, 114, 115])");
}
