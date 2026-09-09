//! `INSERT IGNORE` on a CHECK-violating row: Go's `batchCheckAndInsert`
//! downgrades `ErrCheckConstraintViolated` to a warning and SKIPS the row,
//! exactly like a duplicate key (`insert_common.go:1364-1370`); a plain
//! insert still fails the statement with 3819.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    let settings = CreateTableSettings {
        enable_check_constraint: true,
        ..Default::default()
    };
    ddl::run_create_table_in(
        "create table t (a int check (a > 0))",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn insert_ignore_skips_check_violating_rows() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);

    // The plain insert rejects (pinned separately); IGNORE skips instead.
    run_insert_on("insert into t values (1)", &mut catalog, &strict)
        .expect("the conforming baseline row inserts");
    let error = run_insert_on("insert into t values (-5)", &mut catalog, &strict)
        .expect_err("the plain insert still fails");
    assert_eq!(error.to_string(), "Check constraint 't_chk_1' is violated.");

    run_insert_on("insert ignore into t values (-5)", &mut catalog, &ignore)
        .expect("IGNORE downgrades the 3819 to a warning");
    run_insert_on("insert ignore into t values (2)", &mut catalog, &ignore)
        .expect("the conforming row under IGNORE passes");

    let rows = run_select_on("select a from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 2, "only the conforming rows are stored");
    assert_eq!(format!("{:?}", rows[1][0]), "Int(2)");
}
