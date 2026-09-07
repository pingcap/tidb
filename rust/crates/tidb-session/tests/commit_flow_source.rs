//! Commit persistence: both `START TRANSACTION ... COMMIT` and
//! `BEGIN ... COMMIT` keep the transaction's writes — the row survives the
//! transaction boundary.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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
fn committed_writes_persist() {
    let mut session = Session::new();
    session.run("create table u (a int primary key)").unwrap();

    session.run("start transaction").unwrap();
    session.run("insert into u values (1)").unwrap();
    session.run("commit").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "1");

    session.run("begin").unwrap();
    session.run("insert into u values (2)").unwrap();
    session.run("commit").unwrap();
    assert_eq!(rows(&mut session, "select a from u order by a"), "1;2");
}
