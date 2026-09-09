//! `REPLACE INTO` with a CHECK-violating new row: Go deletes and re-adds
//! inside ONE transaction, so the addRecord failure (3819) rolls the
//! statement back and the conflicting row SURVIVES. The port validates the
//! candidate before any deletion to the same observable end.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

#[test]
fn failed_replace_leaves_the_old_row() {
    let mut catalog = Catalog::default();
    let settings = CreateTableSettings {
        enable_check_constraint: true,
        ..Default::default()
    };
    ddl::run_create_table_in(
        "create table t (a int primary key, b int check (b > 0))",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 5)", &mut catalog, &strict).unwrap();

    let error = run_insert_on("replace into t values (1, -1)", &mut catalog, &strict)
        .expect_err("the violating REPLACE must fail");
    assert_eq!(error.to_string(), "Check constraint 't_chk_1' is violated.");

    // The statement rolled back: the conflicting row is STILL there.
    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1, "the old row survives the failed REPLACE");
    assert_eq!(format!("{:?}", rows[0][1]), "Int(5)");
}
