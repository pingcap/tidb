//! SEQUENCE function family: `NEXTVAL(seq)` allocates 1, 2 (default
//! increment), `LASTVAL(seq)` answers the last allocated value,
//! `SETVAL(seq, 50)` repositions and reports 50, the next allocation
//! continues at 51, zero-arg `LASTVAL()` fails Go's 1582 (arity 1,1), and
//! DROP SEQUENCE removes the sequence.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
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

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn sequence_function_family() {
    let mut session = Session::new();
    session.run("create sequence seq").unwrap();

    // NEXTVAL allocates 1, 2 with the default increment.
    assert_eq!(rows(&mut session, "select nextval(seq)"), "1");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "2");

    // LASTVAL(seq) answers the last allocation.
    assert_eq!(rows(&mut session, "select lastval(seq)"), "2");

    // SETVAL(seq, 50) repositions and reports 50; the next allocation is 51.
    assert_eq!(rows(&mut session, "select setval(seq, 50)"), "50");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "51");

    // Zero-arg LASTVAL fails Go's arity (1,1) with 1582.
    assert!(error(&mut session, "select lastval()").contains("Incorrect parameter count"),);

    // DROP SEQUENCE removes it.
    session.run("drop sequence seq").unwrap();
    assert!(error(&mut session, "select nextval(seq)").contains("seq"));
}
