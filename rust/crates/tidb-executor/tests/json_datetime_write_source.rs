//! JSON and DATETIME write validation: a JSON literal parses to the binary
//! form (normalized, keys sorted), invalid JSON fails with Go's "Invalid
//! JSON text" (3141), loose temporal input normalizes ('2024-2-3' accepted),
//! and an impossible date fails with "Incorrect datetime value" (1292).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, j json, dt datetime)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn json_and_datetime_writes_validate() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = setup();

    run_insert_on(
        r#"insert into t values (1, '{"b": 2, "a": 1}', '2024-2-3')"#,
        &mut catalog,
        &strict,
    )
    .unwrap();
    let rows = run_select_on("select dt, cast(j as char) from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0]), "Time(Time { core: {2024 2 3 0 0 0 0}, kind: DateTime, fsp: 0 })");
    assert_eq!(
        format!("{:?}", rows[0][1].as_raw_bytes()),
        "Some([123, 34, 97, 34, 58, 32, 49, 44, 32, 34, 98, 34, 58, 32, 50, 125])",
        "normalized to sorted-key JSON"
    );

    let error = run_insert_on(
        r#"insert into t values (2, '{bad}', '2024-01-01')"#,
        &mut catalog,
        &strict,
    )
    .expect_err("invalid JSON is rejected");
    assert!(
        error.to_string().contains("Invalid JSON text"),
        "{error}"
    );

    let error = run_insert_on(
        r#"insert into t values (3, '{}', '2024-13-40')"#,
        &mut catalog,
        &strict,
    )
    .expect_err("an impossible date is rejected");
    assert!(
        error
            .to_string()
            .contains("Incorrect datetime value: '2024-13-40' for column 'dt' at row 1"),
        "{error}"
    );
}
