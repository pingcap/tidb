//! UPDATE affected-rows accounting: by default only rows whose new value
//! DIFFERS count; a client that negotiated `CLIENT_FOUND_ROWS` sees matching
//! rows instead (`StmtContext::client_found_rows`).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_update_on, Catalog, CreateTableSettings,
    StmtContext,
};

#[test]
fn update_accounting_modes() {
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
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();

    // A value that CHANGES: one row.
    let changed = run_update_on("update t set b = 'xyz' where a = 1", &mut catalog, &strict)
        .unwrap();
    assert_eq!(changed, 1);

    // The SAME value again: nothing counts by default.
    let same = run_update_on("update t set b = 'xyz' where a = 1", &mut catalog, &strict)
        .unwrap();
    assert_eq!(same, 0);

    // With CLIENT_FOUND_ROWS: the matching row counts even unchanged.
    let found_ctx = StmtContext::for_dml(false, true, false).with_client_found_rows(true);
    let found = run_update_on("update t set b = 'xyz' where a = 1", &mut catalog, &found_ctx)
        .unwrap();
    assert_eq!(found, 1);
}
