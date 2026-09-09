//! BINARY and YEAR write casts: a short BINARY(4) value zero-pads, YEAR
//! accepts 1901-2155 (and 0), a strict out-of-range year fails with Go's
//! "Out of range value" (1264), and IGNORE clamps to the boundary 2155.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, bin binary(4), y year)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn binary_padding_and_year_boundaries() {
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);
    let mut catalog = setup();

    run_insert_on("insert into t values (1, 'ab', 2024)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select bin, y from t", &catalog, &strict).unwrap();
    assert_eq!(
        format!("{:?}", rows[0][0].as_raw_bytes()),
        "Some([97, 98, 0, 0])",
        "BINARY(4) pads with zero bytes"
    );
    assert_eq!(format!("{:?}", rows[0][1]), "Int(2024)");

    let error = run_insert_on("insert into t values (2, 'xy', 2156)", &mut catalog, &strict)
        .expect_err("2156 is outside YEAR's range");
    assert!(
        error.to_string().contains("Out of range value for column 'y' at row 1"),
        "{error}"
    );

    run_insert_on(
        "insert ignore into t values (2, 'xy', 2156)",
        &mut catalog,
        &ignore,
    )
    .expect("IGNORE clamps the year");
    let rows = run_select_on("select y from t where a = 2", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][0]), "Int(2155)", "clamped to the boundary");
}
