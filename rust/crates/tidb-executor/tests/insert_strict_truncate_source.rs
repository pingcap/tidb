use tidb_executor::{
    ddl, run_insert_on, Catalog, CreateTableSettings, StmtContext,
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
fn strict_insert_too_long_is_rejected_with_1406() {
    let mut catalog = setup();
    // STRICT_TRANS_TABLES (TiDB's default mode): the write fails.
    let ctx = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 'abc')", &mut catalog, &ctx)
        .expect("in-range value inserts");
    let err = run_insert_on("insert into t values (2, 'abcdef')", &mut catalog, &ctx)
        .err()
        .expect("strict mode must refuse the over-long value");
    assert_eq!(
        err.to_string(),
        "Data too long for column 'b' at row 1",
        "Go: ErrDataTooLong via ProduceStrWithSpecifiedTp"
    );
}

#[test]
fn non_strict_insert_too_long_truncates_with_warning() {
    let mut catalog = setup();
    // sql_mode = '': the write truncates to the declared flen and warns.
    let ctx = StmtContext::default();
    run_insert_on("insert into t values (1, 'abcdef')", &mut catalog, &ctx)
        .expect("non-strict mode stores the truncated value");
}
