//! Subquery predicates in DELETE: `a IN (select ...)` removes exactly the
//! matching rows, `EXISTS` correlates per row, and `NOT IN` keeps the
//! matching rows — all evaluated against the subquery's own tables.

use tidb_executor::{
    ddl, run_create_table_in, run_delete_on, run_insert_on, run_select_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table s (k int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn in_and_exists_subqueries_drive_deletes() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = setup();
    run_insert_on("insert into t values (1), (2), (3)", &mut catalog, &strict).unwrap();
    run_insert_on("insert into s values (2), (3)", &mut catalog, &strict).unwrap();

    let removed = run_delete_on(
        "delete from t where exists (select 1 from s where s.k = t.a)",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(removed, 2);
    let rows = run_select_on("select a from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1)]]");

    run_insert_on("insert into t values (2), (3)", &mut catalog, &strict).unwrap();
    let removed = run_delete_on(
        "delete from t where a in (select k from s)",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(removed, 2);
    let rows = run_select_on("select a from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1)]]");

    let removed = run_delete_on(
        "delete from t where a not in (select k from s)",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(removed, 1, "row 1 is the only one missing from the subquery");
    let rows = run_select_on("select a from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[]");
}
