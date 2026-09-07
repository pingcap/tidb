//! The sequence helper functions: LASTVAL(seq) answers the last allocated
//! value, SETVAL(seq, n) returns the new value when it advances the
//! position (Go `SetSequenceVal`) and NULL when the position already
//! satisfies it, and zero-arg LASTVAL fails with Go's 1582 (Go's arity is
//! exactly 1: `baseFunctionClass{ast.LastVal, 1, 1}`).

use tidb_session::Session;

fn one(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn lastval_setval_and_arity() {
    let mut session = Session::new();
    session.run("create sequence seq").unwrap();

    assert_eq!(one(&mut session, "select nextval(seq)"), "1");
    assert_eq!(one(&mut session, "select lastval(seq)"), "1", "the last allocated value");

    // A forward SETVAL reports the new value...
    assert_eq!(one(&mut session, "select setval(seq, 100)"), "100");
    // ...and the NEXT allocation continues past it (NOT 100 itself).
    assert_eq!(one(&mut session, "select nextval(seq)"), "101");

    // A backwards SETVAL is a no-op reported as NULL (Go's isNull return).
    assert_eq!(one(&mut session, "select setval(seq, 5)"), "NULL");
    assert_eq!(one(&mut session, "select nextval(seq)"), "102");

    // Zero-arg LASTVAL: Go's arity is exactly 1 -> 1582.
    let error = session
        .run("select lastval()")
        .expect_err("LASTVAL requires the sequence argument");
    assert!(
        error
            .to_string()
            .contains("Incorrect parameter count in the call to native function 'lastval'"),
        "{error}"
    );
}
