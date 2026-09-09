//! FK-guarded table lifecycle: with `foreign_key_checks` on, TRUNCATE of a
//! referenced parent fails with Go's ErrTruncateIllegalForeignKey text and
//! DROP with ErrForeignKeyCannotDropParent's, both naming the child.

use tidb_executor::{
    ddl, run_create_table_in, run_drop_table_in, run_insert_on, run_truncate_table_in, Catalog,
    CreateTableSettings, StmtContext,
};

fn setup() -> Catalog {
    let mut catalog = Catalog::default();
    let settings = CreateTableSettings {
        foreign_key_checks: true,
        ..Default::default()
    };
    ddl::run_create_table_in(
        "create table p (a int primary key)",
        &mut catalog,
        "test",
        settings.clone(),
        &StmtContext::for_query(),
    )
    .unwrap();
    ddl::run_create_table_in(
        "create table c (x int primary key, y int, foreign key (y) references p(a))",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn truncate_of_a_referenced_parent_is_rejected() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into p values (1)", &mut catalog, &strict).unwrap();
    run_insert_on("insert into c values (10, 1)", &mut catalog, &strict).unwrap();

    let error = run_truncate_table_in("truncate table p", &mut catalog, "test", strict.sql_mode())
        .expect_err("Go: ErrTruncateIllegalForeignKey");
    let rendered = error.to_string();
    assert!(
        rendered.contains("Cannot truncate a table referenced in a foreign key constraint"),
        "{rendered}"
    );
    assert!(rendered.contains("test`.`c"), "the child is named: {rendered}");
}

#[test]
fn drop_of_a_referenced_parent_is_rejected() {
    let mut catalog = setup();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into p values (1)", &mut catalog, &strict).unwrap();
    run_insert_on("insert into c values (10, 1)", &mut catalog, &strict).unwrap();

    let error = run_drop_table_in(
        "drop table p",
        &mut catalog,
        "test",
        strict.sql_mode(),
        true,
    )
    .expect_err("Go: ErrForeignKeyCannotDropParent");
    let rendered = error.to_string();
    assert!(
        rendered.contains("Cannot drop table 'p' referenced by a foreign key constraint 'fk_1' on table 'c'."),
        "{rendered}"
    );
}
