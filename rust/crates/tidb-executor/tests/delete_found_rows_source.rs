//! DELETE affected-rows accounting and the FOUND_ROWS() flow: a DELETE
//! reports the rows it removed (0 when nothing matched), and FOUND_ROWS()
//! answers how many rows the PRECEDING SELECT actually returned.

use tidb_executor::{
    ddl, run_create_table_on, run_delete_on, run_insert_on, run_select_on, Catalog,
    CreateTableSettings, StmtContext,
};

#[test]
fn delete_accounting_and_found_rows() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'a'), (2, 'b'), (3, 'c')",
        &mut catalog,
        &strict,
    )
    .unwrap();

    // A DELETE reports the rows it removed.
    let removed = run_delete_on("delete from t where a <= 2", &mut catalog, &strict).unwrap();
    assert_eq!(removed, 2);

    // ... and 0 when nothing matched.
    let none = run_delete_on("delete from t where a = 99", &mut catalog, &strict).unwrap();
    assert_eq!(none, 0);

    // FOUND_ROWS() reads what the preceding SELECT returned.
    let rows = run_select_on("select * from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1);
    let q = StmtContext::for_query().with_last_found_rows(rows.len() as u64);
    match run_select_on("select found_rows()", &catalog, &q).unwrap()[0][0] {
        tidb_datatype::Datum::UInt(n) => assert_eq!(n, 1),
        ref other => panic!("expected a UInt count, got {other:?}"),
    }
}
