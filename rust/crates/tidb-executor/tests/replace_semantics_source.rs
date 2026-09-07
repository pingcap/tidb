//! `REPLACE INTO`: on a duplicate key the old row is deleted and the new one
//! inserted — Go reports 2 affected rows (the delete plus the insert), and
//! the surviving row carries the new values.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

#[test]
fn replace_into_replaces_the_duplicate_row() {
    let mut catalog = Catalog::default();
    let strict = StmtContext::for_dml(false, true, false);
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();

    let affected = run_insert_on("replace into t values (1, 'xyz')", &mut catalog, &strict)
        .expect("REPLACE succeeds on a duplicate key");
    assert_eq!(affected, 2, "Go counts the delete and the insert as 2 rows");

    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1, "exactly one row survives");
    assert_eq!(format!("{:?}", rows[0][1].as_raw_bytes()), "Some([120, 121, 122])");
}
