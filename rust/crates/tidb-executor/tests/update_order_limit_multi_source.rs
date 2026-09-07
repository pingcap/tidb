//! Two UPDATE statement shapes: `ORDER BY ... LIMIT` updates only the first
//! n rows in the given order, and a multi-table `UPDATE t, u ... WHERE`
//! joins the tables and applies to the joined rows only.

use tidb_executor::{
    ddl, run_create_table_in, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn table(sql: &str, catalog: &mut Catalog) {
    ddl::run_create_table_in(
        sql,
        catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
}

#[test]
fn order_by_limit_updates_first_n_rows() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    table("create table t (a int primary key, b int)", &mut catalog);
    run_insert_on("insert into t values (1, 0), (2, 0), (3, 0)", &mut catalog, &strict).unwrap();

    let changed = run_update_on(
        "update t set b = 9 order by a desc limit 2",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(changed, 2);

    let rows = run_select_on("select a, b from t order by a", &catalog, &strict).unwrap();
    assert_eq!(
        format!("{:?}", rows),
        "[[Int(1), Int(0)], [Int(2), Int(9)], [Int(3), Int(9)]]",
        "the DESC order updates rows 3 and 2, leaving row 1"
    );
}

#[test]
fn multi_table_update_joins_and_filters() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    table("create table t (a int primary key, b int)", &mut catalog);
    table("create table u (a int primary key, label varchar(4))", &mut catalog);
    run_insert_on("insert into t values (1, 0), (2, 0)", &mut catalog, &strict).unwrap();
    run_insert_on("insert into u values (1, 'x'), (2, 'y')", &mut catalog, &strict).unwrap();

    let changed = run_update_on(
        "update t, u set t.b = 5 where t.a = u.a and u.label = 'y'",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(changed, 1, "only the join row with label 'y' updates");

    let rows = run_select_on("select a, b from t order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1), Int(0)], [Int(2), Int(5)]]");
}
