//! DECIMAL and unsigned-TINYINT boundary writes: scale rounding is silent
//! and accepted (1.005 -> 1.01 in DECIMAL(4,2)), an overflow fails strict
//! with Go's "Out of range value" (1264), and IGNORE clamps to the type
//! boundaries (99.99 / 255).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, d decimal(4,2), ti tinyint unsigned)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn rounding_and_boundary_clamping() {
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);
    let mut catalog = setup();

    // Rounding to scale is silent and accepted.
    run_insert_on("insert into t values (1, 1.005, 200)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select d, ti from t", &catalog, &strict).unwrap();
    assert!(format!("{:?}", rows[0][0]).contains("49, 48, 49"), "1.01 stored: {:?}", rows[0][0]);
    assert_eq!(format!("{:?}", rows[0][1]), "UInt(200)");

    // Overflow fails strict, naming the column.
    let error = run_insert_on("insert into t values (2, 100.00, 300)", &mut catalog, &strict)
        .expect_err("both values overflow");
    assert!(
        error.to_string().contains("Out of range value for column 'd' at row 1"),
        "{error}"
    );

    // IGNORE clamps both to their boundaries.
    run_insert_on(
        "insert ignore into t values (2, 100.00, 300)",
        &mut catalog,
        &ignore,
    )
    .expect("IGNORE clamps");
    let rows = run_select_on("select d, ti from t where a = 2", &catalog, &strict).unwrap();
    assert!(format!("{:?}", rows[0][0]).contains("57, 57, 57, 57"), "99.99 stored: {:?}", rows[0][0]);
    assert_eq!(format!("{:?}", rows[0][1]), "UInt(255)");
}
