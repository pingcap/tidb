//! ENUM/SET write casts: string and numeric literals resolve to members
//! ('y'==2, 3=='z'; SET 'p,r'==5, 2=='q'), an out-of-range ENUM index fails
//! strict with Go's "Data truncated" (1265), IGNORE stores the empty enum,
//! and an unknown SET member truncates strictly.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, e enum('x','y','z'), s set('p','q','r'))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn enum_set_string_and_numeric_literals() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 'y', 'p,r')", &mut catalog, &strict).unwrap();
    run_insert_on("insert into t values (2, 3, 2)", &mut catalog, &strict).unwrap();
    let rows = run_select_on("select a, e, s from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows[0][1]), "Enum(MysqlEnum { name: GoString([121]), value: 2 }, Utf8Mb4Bin)");
    assert_eq!(format!("{:?}", rows[0][2]), "Set(MysqlSet { name: GoString([112, 44, 114]), value: 5 }, Utf8Mb4Bin)");
    assert_eq!(format!("{:?}", rows[1][1]), "Enum(MysqlEnum { name: GoString([122]), value: 3 }, Utf8Mb4Bin)");
    assert_eq!(format!("{:?}", rows[1][2]), "Set(MysqlSet { name: GoString([113]), value: 2 }, Utf8Mb4Bin)");
}

#[test]
fn out_of_range_enum_truncates_or_stores_empty() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let ignore = StmtContext::for_dml(false, true, true);

    let error = run_insert_on("insert into t values (3, 9, 'p')", &mut catalog, &strict)
        .expect_err("ENUM index 9 is out of range");
    assert!(
        error.to_string().contains("Data truncated for column 'e' at row 1"),
        "{error}"
    );

    run_insert_on(
        "insert ignore into t values (3, 9, 'p')",
        &mut catalog,
        &ignore,
    )
    .expect("IGNORE stores the empty enum");
    let rows = run_select_on("select e from t where a = 3", &catalog, &strict).unwrap();
    assert_eq!(
        format!("{:?}", rows[0][0]),
        "Enum(MysqlEnum { name: GoString([]), value: 0 }, Utf8Mb4Bin)"
    );
}

#[test]
fn unknown_set_member_truncates_strictly() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    let error = run_insert_on("insert into t values (4, 'x', 'p,z')", &mut catalog, &strict)
        .expect_err("'z' is not a SET member");
    assert!(
        error.to_string().contains("Data truncated for column 's' at row 1"),
        "{error}"
    );
}
