//! SEQUENCE lifecycle: `CREATE SEQUENCE ... START 10 INCREMENT 5` hands out
//! 10, 15, 20 through `NEXTVAL(seq)`, and DROP SEQUENCE removes it.

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

#[test]
fn sequence_allocates_by_increment_and_drops() {
    let mut session = Session::new();
    session
        .run("create sequence seq start 10 increment 5")
        .unwrap();

    assert_eq!(rows(&mut session, "select nextval(seq)"), "10");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "15");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "20");

    session.run("drop sequence seq").unwrap();
    let error = session
        .run("select nextval(seq)")
        .expect_err("the sequence is gone");
    assert!(error.to_string().contains("seq"), "{error}");
}
