//! LASTVAL before any NEXTVAL in the session returns NULL — Go's
//! `SequenceState.GetLastValue` (pkg/sessionctx/variable/sequence_state.go
//! :42-52) returns the "not cached" flag and the builtin emits NULL — and
//! the state populates after the first NEXTVAL.

use tidb_session::Session;

#[test]
fn lastval_is_null_before_first_nextval() {
    let mut session = Session::new();
    session.run("create sequence s start with 10").unwrap();

    match session.run("select lastval(s)").unwrap() {
        tidb_session::StmtResult::Rows(rows) => {
            assert_eq!(format!("{:?}", rows[0][0]), "Null");
        }
        other => panic!("{other:?}"),
    }

    // NEXTVAL populates the session state; LASTVAL then returns 10.
    match session.run("select nextval(s)").unwrap() {
        tidb_session::StmtResult::Rows(rows) => {
            assert_eq!(format!("{:?}", rows[0][0]), "Int(10)");
        }
        other => panic!("{other:?}"),
    }
    match session.run("select lastval(s)").unwrap() {
        tidb_session::StmtResult::Rows(rows) => {
            assert_eq!(format!("{:?}", rows[0][0]), "Int(10)");
        }
        other => panic!("{other:?}"),
    }
}
