//! `ROW_COUNT()` reads `PrevAffectedRows`, promoted at the statement
//! boundary: INSERT/UPDATE/DELETE publish their affected count, SELECT
//! publishes -1, everything else 0 (select.go:1234-1240). The published
//! count is the same affected-rows accounting the OK packet reports -- so
//! an ODKU duplicate update answers 2 and a no-op UPDATE answers 0.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
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

fn row_count(catalog: &Catalog, prev_affected: i64) -> i64 {
    let q = StmtContext::for_query().with_previous_statement(0, prev_affected);
    match run_select_on("select row_count()", catalog, &q).unwrap()[0][0] {
        tidb_datatype::Datum::Int(n) => n,
        ref other => panic!("expected Int, got {other:?}"),
    }
}

#[test]
fn row_count_flow() {
    let mut catalog = setup();

    // A two-row INSERT publishes 2.
    let n = run_insert_on(
        "insert into t values (1, 'a'), (2, 'b')",
        &mut catalog,
        &StmtContext::for_dml(false, true, false),
    )
    .unwrap();
    assert_eq!(row_count(&catalog, n as i64), 2);

    // An ODKU duplicate takes the update branch: 2 affected rows, and
    // ROW_COUNT() reports the same accounting.
    let n = run_insert_on(
        "insert into t values (1, 'x') on duplicate key update b = 'upd'",
        &mut catalog,
        &StmtContext::for_dml(false, true, false),
    )
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(row_count(&catalog, n as i64), 2);

    // A SELECT publishes -1.
    let _ = run_select_on(
        "select * from t",
        &catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(row_count(&catalog, -1), -1);

    // A no-op UPDATE publishes 0.
    let n = run_update_on(
        "update t set b = b where a = 99",
        &mut catalog,
        &StmtContext::for_dml(false, true, false),
    )
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(row_count(&catalog, n as i64), 0);
}
