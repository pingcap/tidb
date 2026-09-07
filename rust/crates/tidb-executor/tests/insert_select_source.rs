//! `INSERT ... SELECT`: the source query runs over its own tables and the
//! projected values (computed expressions, WHERE filtering) land in the
//! target, with Go's affected-rows accounting.

use tidb_executor::{
    ddl, run_create_table_in, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table src (a int primary key, b varchar(8))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table dst (a int primary key, b varchar(8))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn insert_select_with_computed_projection() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = setup();
    run_insert_on(
        "insert into src values (1, 'one'), (2, 'two'), (3, 'three')",
        &mut catalog,
        &strict,
    )
    .unwrap();

    let affected = run_insert_on(
        "insert into dst select a + 10, concat(b, '!') from src where a <= 2",
        &mut catalog,
        &strict,
    )
    .unwrap();
    assert_eq!(affected, 2, "the WHERE filters row 3 out");

    let keys = run_select_on("select a from dst order by a", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", keys[0][0]), "Int(11)");
    assert_eq!(format!("{:?}", keys[1][0]), "Int(12)");
    let names = run_select_on("select b from dst order by a", &catalog, &strict).unwrap();
    assert_eq!(
        format!("{:?}", names[0][0].as_raw_bytes()),
        "Some([111, 110, 101, 33])",
        "'one!' stored"
    );
}
