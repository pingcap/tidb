//! NULL into a NOT NULL column: strict mode rejects with Go's 1048 message
//! ("Column 'b' cannot be null"); INSERT IGNORE downgrades the error to a
//! warning and stores the column's implicit default (`''` for varchar).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3) not null)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn strict_null_into_not_null_is_rejected() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let error = run_insert_on("insert into t values (1, NULL)", &mut catalog, &strict)
        .expect_err("strict mode refuses NULL into a NOT NULL column");
    assert_eq!(
        error.to_string(),
        "Column 'b' cannot be null",
        "Go: ErrBadNull (1048)"
    );
}

#[test]
fn ignore_null_into_not_null_stores_the_implicit_default() {
    let mut catalog = setup();
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on("insert ignore into t values (1, NULL)", &mut catalog, &ignore)
        .expect("IGNORE downgrades the 1048 to a warning");
    let rows = run_select_on("select a, b from t", &catalog, &ignore).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        format!("{:?}", rows[0][1].as_raw_bytes()),
        "Some([])",
        "the implicit default '' is stored"
    );
}
