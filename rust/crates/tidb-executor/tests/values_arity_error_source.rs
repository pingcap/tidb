//! VALUES-arity mismatches report Go's coded `ErrWrongValueCountOnRow`
//! (1136, "Column count doesn't match value count at row %d"): row 1 answers
//! to the column list (planbuilder.go:4349), a later row answers to the
//! first row's width (:4361) and INSERT SELECT always reports row 1 (:4474).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, Catalog, CreateTableSettings, StmtContext,
};

#[test]
fn arity_mismatch_caries_the_go_1136_error() {
    let strict = StmtContext::for_dml(false, true, false);
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int primary key, b varchar(8))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();

    // A short row: row 1, answering to the two-column list.
    let error = run_insert_on("insert into t values (1)", &mut catalog, &strict)
        .expect_err("a short row is rejected");
    let coded = error.clone().to_mysql_error();
    assert_eq!(coded.code, 1136, "Go: ErrWrongValueCountOnRow");
    assert_eq!(
        coded.message, "Column count doesn't match value count at row 1",
        "Go's errname.go message names the offending row"
    );

    // A long row: still row 1.
    let error = run_insert_on("insert into t values (1, 'x', 'y')", &mut catalog, &strict)
        .expect_err("a long row is rejected");
    let coded = error.clone().to_mysql_error();
    assert_eq!(coded.code, 1136);
    assert_eq!(coded.message, "Column count doesn't match value count at row 1");

    // "insert into t values (1, 'x'), (2)": the SECOND row is the offender
    // and names itself (Go :4361).
    let error = run_insert_on("insert into t values (1, 'x'), (2)", &mut catalog, &strict)
        .expect_err("a later narrower row is rejected");
    let coded = error.clone().to_mysql_error();
    assert_eq!(coded.code, 1136);
    assert_eq!(coded.message, "Column count doesn't match value count at row 2");
}
