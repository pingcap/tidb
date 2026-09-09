//! JOIN ... USING (id): the shared column merges (selectable by bare name
//! without a qualifier), rows match on it, and the unmatched outer rows
//! drop out of the inner join.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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

fn setup(session: &mut Session) {
    session.run("create table l (id int, lv int)").unwrap();
    session.run("create table r (id int, rv int)").unwrap();
    session.run("insert into l values (1, 10), (2, 20)").unwrap();
    session.run("insert into r values (1, 100), (3, 300)").unwrap();
}

#[test]
fn using_clause_merges_the_shared_column() {
    let mut session = Session::new();
    setup(&mut session);

    // Qualified per-side names do not exist after USING; bare `id` answers.
    assert_eq!(
        rows(&mut session, "select id, lv, rv from l join r using (id) order by id"),
        "i:1|i:10|i:100"
    );
    // SELECT * shows the merged column once.
    assert_eq!(
        rows(&mut session, "select * from l join r using (id) order by id"),
        "i:1|i:10|i:100"
    );
}
