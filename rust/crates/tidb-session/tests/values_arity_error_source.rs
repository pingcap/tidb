//! VALUES-arity mismatches report Go's coded `ErrWrongValueCountOnRow`
//! (1136, "Column count doesn't match value count at row %d"): row 1 answers
//! to the column list (planbuilder.go:4349), a later row answers to the
//! first row's width (:4361) and INSERT SELECT always reports row 1 (:4474).

use crate::support::testkit::TestKit;

#[test]
fn arity_mismatch_caries_the_go_1136_error() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(8))");

    // A short row: row 1, answering to the two-column list. Go's errname.go
    // message names the offending row.
    let error = tk.must_get_err_code("insert into t values (1)", 1136);
    assert_eq!(error.message, "Column count doesn't match value count at row 1");

    // A long row: still row 1.
    let error = tk.must_get_err_code("insert into t values (1, 'x', 'y')", 1136);
    assert_eq!(error.message, "Column count doesn't match value count at row 1");

    // "insert into t values (1, 'x'), (2)": the SECOND row is the offender
    // and names itself (Go :4361).
    let error = tk.must_get_err_code("insert into t values (1, 'x'), (2)", 1136);
    assert_eq!(error.message, "Column count doesn't match value count at row 2");
}
