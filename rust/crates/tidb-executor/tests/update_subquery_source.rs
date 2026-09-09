//! Correlated scalar subquery in an UPDATE's SET: each row's assignment
//! evaluates the subquery against that row's own values — `max(v)` per
//! matching `k` lands per row.

use tidb_executor::{
    ddl, run_create_table_in, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

#[test]
fn correlated_subquery_drives_the_assignment() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table s (k int, v int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on("insert into t values (1, 0), (2, 0)", &mut catalog, &strict).unwrap();
    run_insert_on(
        "insert into s values (1, 111), (1, 5), (2, 222)",
        &mut catalog,
        &strict,
    )
    .unwrap();

    let affected = run_update_on(
        "update t set b = (select max(v) from s where s.k = t.a)",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(affected, 2);

    let rows = run_select_on("select a, b from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1), Int(111)], [Int(2), Int(222)]]");
}
