//! Current Go `pkg/expression/builtin_info.go` and
//! `pkg/executor/adapter.go` contracts for `FOUND_ROWS()`.

use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput, StmtResult};

#[test]
fn found_rows_reports_the_previous_completed_result_set() {
    let mut session = Session::new();
    session.run("CREATE TABLE fr (v BIGINT)").unwrap();
    session.run("INSERT INTO fr VALUES (1), (2), (3)").unwrap();

    assert_eq!(
        session.run("SELECT v FROM fr ORDER BY v LIMIT 2").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );
    let StmtOutput::Rows { columns, rows } =
        session.run_with_columns("SELECT FOUND_ROWS()").unwrap()
    else {
        panic!("FOUND_ROWS did not return a result set")
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::LongLong);
    assert!(columns[0].1.is_unsigned());
    assert_eq!(rows, vec![vec![Datum::UInt(2)]]);

    // FOUND_ROWS() is itself a one-row result, so it becomes the value seen
    // by the next statement. A non-result statement leaves that publication
    // alone; an empty result replaces it with zero.
    session.run("INSERT INTO fr VALUES (4)").unwrap();
    assert_eq!(
        session.run("SELECT FOUND_ROWS()").unwrap(),
        StmtResult::Rows(vec![vec![Datum::UInt(1)]])
    );
    assert_eq!(
        session.run("SELECT v FROM fr WHERE v > 99").unwrap(),
        StmtResult::Rows(Vec::new())
    );
    assert_eq!(
        session.run("SELECT FOUND_ROWS()").unwrap(),
        StmtResult::Rows(vec![vec![Datum::UInt(0)]])
    );
}
