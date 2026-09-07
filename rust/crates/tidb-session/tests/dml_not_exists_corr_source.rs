//! Correlated EXISTS/NOT EXISTS inside DML WHERE clauses: DELETE with NOT
//! EXISTS removes only unmatched rows; UPDATE with EXISTS moves only the
//! matched row (id 2 -> 12).

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table l (id int)").unwrap();
    session.run("insert into l values (1), (2), (3)").unwrap();
    session.run("create table r (id int)").unwrap();
    session.run("insert into r values (2)").unwrap();
}

#[test]
fn dml_not_exists_and_exists() {
    let mut session = Session::new();
    seed(&mut session);

    // DELETE keeps only rows with a match in r.
    let removed = match session
        .run("delete from l where not exists (select 1 from r where r.id = l.id)")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 2);
    assert_eq!(rows(&mut session, "select id from l order by id"), "2");

    // UPDATE with EXISTS: only the matched row moves.
    session.run("insert into l values (1), (3)").unwrap();
    let moved = match session
        .run("update l set id = id + 10 where exists (select 1 from r where r.id = l.id)")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(moved, 1);
    assert_eq!(rows(&mut session, "select id from l order by id"), "1;3;12");
}
