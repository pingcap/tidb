//! INSERT/UPDATE IGNORE write semantics beyond the truncate downgrade:
//! duplicate-key rows are SKIPPED (not stored) with a warning, and NULL into
//! a nullable column stays NULL — Go `insert ignore` via `HandleErrorWithAlias`.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(3))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn insert_ignore_duplicate_key_skips_the_row() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &strict).unwrap();
    run_insert_on("insert ignore into t values (1, 'xyz')", &mut catalog, &ignore)
        .expect("IGNORE skips the duplicate with a warning");
    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(rows.len(), 1, "the duplicate row is NOT stored");
    assert_eq!(format!("{:?}", rows[0][1].as_raw_bytes()), "Some([97, 98, 99])");
}

#[test]
fn update_ignore_truncates_over_long_value() {
    let mut catalog = setup();
    let ignore = StmtContext::for_dml(false, true, true);
    run_insert_on("insert into t values (1, 'zzz')", &mut catalog, &ignore).unwrap();
    run_update_on("update t set b = 'abcdef' where a = 1", &mut catalog, &ignore)
        .expect("UPDATE IGNORE truncates with a warning");
    let rows = run_select_on("select b from t", &catalog, &ignore).unwrap();
    assert_eq!(format!("{:?}", rows[0][0].as_raw_bytes()), "Some([97, 98, 99])");
}
