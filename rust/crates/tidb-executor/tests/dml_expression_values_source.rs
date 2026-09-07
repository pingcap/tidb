//! DML evaluates scalar expressions in VALUES and SET: constants compose
//! (`1 + 1`, `concat('ab', 'cd')`, `upper('xy')`), and an UPDATE may
//! reference its own column (`a = a * 3`) — the NEW value feeds later
//! assignments of the same row.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

#[test]
fn computed_values_in_insert_and_update() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(8))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();

    run_insert_on(
        "insert into t values (1 + 1, concat('ab', 'cd'))",
        &mut catalog,
        &strict,
    )
    .unwrap();
    run_update_on(
        "update t set a = a * 3, b = concat(b, '!') where a = 2",
        &mut catalog,
        &strict,
    )
    .unwrap();
    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][1].as_raw_bytes()), "Some([97, 98, 99, 100, 33])");

    run_insert_on("insert into t values (9, upper('xy'))", &mut catalog, &strict).unwrap();
    run_update_on("update t set b = left(b, 2) where a = 9", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select b from t where a = 9", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0].as_raw_bytes()), "Some([88, 89])");
}
