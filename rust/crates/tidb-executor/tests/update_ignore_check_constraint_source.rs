//! `UPDATE IGNORE` on a CHECK-violating assignment: Go's update executor
//! downgrades `ErrCheckConstraintViolated` the same way it downgrades a
//! duplicate key — warning, row skipped, statement succeeds
//! (update.go:335-341, via `HandleErrorWithAlias`). A plain UPDATE fails
//! with 3819.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
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
    catalog
}

#[test]
fn update_ignore_skips_check_violating_rows() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 5)", &mut catalog, &strict).unwrap();

    // The plain UPDATE fails the statement with Go's 3819.
    let error = run_update_on("update t set b = -1 where a = 1", &mut catalog, &strict)
        .expect_err("a plain UPDATE must fail");
    assert_eq!(error.to_string(), "Check constraint 't_chk_1' is violated.");

    // UPDATE IGNORE downgrades: warning, row skipped, statement succeeds.
    let affected = run_update_on(
        "update ignore t set b = -1 where a = 1",
        &mut catalog,
        &strict,
    )
    .expect("UPDATE IGNORE downgrades the 3819");
    assert_eq!(affected, 0, "the violating row is NOT changed");

    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0]), "Int(5)", "the row is unchanged");
}
