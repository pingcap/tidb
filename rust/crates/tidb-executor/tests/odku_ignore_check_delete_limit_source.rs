//! Two remaining IGNORE/LIMIT flows: `insert ignore ... on duplicate key
//! update` whose UPDATE branch violates a CHECK downgrades to a warning and
//! leaves the stored row alone (insert.go:218), and `DELETE ... LIMIT n`
//! removes exactly the first n matching rows.

use tidb_executor::{
    ddl, run_create_table_on, run_delete_on, run_insert_on, run_select_on, Catalog,
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
fn odku_ignore_skips_a_check_violating_update_branch() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on("insert into t values (1, 5)", &mut catalog, &strict).unwrap();

    run_insert_on(
        "insert ignore into t values (1, -1) on duplicate key update b = -1",
        &mut catalog,
        &ignore,
    )
    .expect("IGNORE downgrades the update branch's 3819");

    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(format!("{:?}", rows[0][0]), "Int(5)", "the stored row is untouched");
}

#[test]
fn delete_limit_stops_after_n_rows() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on(
        "insert into t values (1, 1), (2, 2), (3, 3)",
        &mut catalog,
        &strict,
    )
    .unwrap();

    let removed = run_delete_on("delete from t where a >= 1 limit 2", &mut catalog, &strict)
        .unwrap();
    assert_eq!(removed, 2);

    let rows = run_select_on("select count(*) from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0]), "Int(1)");
}
