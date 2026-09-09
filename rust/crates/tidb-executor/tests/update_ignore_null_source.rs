//! The DEFAULT keyword and UPDATE IGNORE on NOT NULL: `VALUES (..., default)`
//! takes the column's declared default, while `UPDATE ... SET col = NULL`
//! under IGNORE downgrades the 1048 and stores the type's IMPLICIT default
//! (zero value) -- not the declared column default, matching MySQL.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3) not null default 'zz')",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn default_keyword_takes_the_declared_default() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, default)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0].as_raw_bytes()), "Some([122, 122])");
}

#[test]
fn update_ignore_null_stores_the_implicit_default() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();
    let ignore = StmtContext::for_dml(false, true, true);
    run_update_on("update t set b = NULL where a = 1", &mut catalog, &ignore)
        .expect("UPDATE IGNORE downgrades the 1048");
    let rows = run_select_on("select b from t", &catalog, &strict).unwrap();
    assert_eq!(
        format!("{:?}", rows[0][0].as_raw_bytes()),
        "Some([])",
        "the zero value, NOT the declared default"
    );
}
