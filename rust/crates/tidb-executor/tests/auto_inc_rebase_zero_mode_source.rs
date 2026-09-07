//! Two AUTO_INCREMENT statement flows: `UPDATE t SET id = 300` REBASES the
//! allocator (the next allocation lands past 300, Go `updateRecord`), and
//! `NO_AUTO_VALUE_ON_ZERO` stores an explicit 0 as 0 without allocation,
//! with the next allocation continuing above it.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (id int auto_increment primary key, v int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn update_rebases_the_allocator() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = setup();
    run_insert_on("insert into t (v) values (1)", &mut catalog, &strict).unwrap();
    run_update_on("update t set id = 300 where id = 1", &mut catalog, &strict).unwrap();
    run_insert_on("insert into t (v) values (2)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select id from t order by id", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(300)], [Int(301)]]", "the next id lands past 300");
}

#[test]
fn no_auto_value_on_zero_stores_zero() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = setup();
    let zero_mode = StmtContext::for_dml(false, true, false).with_auto_increment_zero_explicit(true);
    run_insert_on("insert into t (id, v) values (0, 1)", &mut catalog, &zero_mode).unwrap();
    let rows = run_select_on("select id from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(0)]]", "0 is STORED, not allocated");

    run_insert_on("insert into t (v) values (2)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select id from t order by id", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(0)], [Int(1)]]", "allocation continues above 0");
}
