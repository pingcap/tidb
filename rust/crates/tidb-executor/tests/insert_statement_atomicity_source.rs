//! Statement atomicity for INSERT: Go runs the write phase inside a
//! transaction, so a failure on a LATER row (duplicate key, CHECK, cast
//! overflow) rolls back the EARLIER rows of the same statement while the
//! AUTO_INCREMENT allocator does NOT rewind -- id gaps persist.

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
fn duplicate_on_a_later_row_rolls_back_earlier_rows() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (9, 'zzz')", &mut catalog, &strict).unwrap();

    let error = run_insert_on(
        "insert into t values (1, 'abc'), (9, 'zzz')",
        &mut catalog, &strict,
    )
    .expect_err("the duplicate must fail the statement");
    assert!(error.to_string().contains("Duplicate entry"), "{error}");

    // The statement rolled back: ONLY the pre-existing row remains.
    let rows = run_select_on("select a from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1, "row 1 of the failed statement must NOT survive");
    assert_eq!(format!("{:?}", rows[0][0]), "Int(9)");
}

#[test]
fn cast_failure_on_a_later_row_keeps_the_table_empty() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let error = run_insert_on(
        "insert into t values (1, 'abc'), (2, 'abcdef')",
        &mut catalog, &strict,
    )
    .expect_err("the over-long value must fail the statement");
    assert!(error.to_string().contains("Data too long"), "{error}");

    let rows = run_select_on("select a from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 0, "the failed statement stores nothing");
}
