//! RIGHT JOIN preserves every right-side row: matched rows carry the
//! left's values, the unmatched right row (id=3) survives with NULLs in
//! the left columns.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
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
fn right_join_preserves_the_right_side() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        rows(
            &mut session,
            "select l.id, lv, r.id, rv from l right join r on l.id = r.id order by r.id"
        ),
        "i:1|i:10|i:1|i:100;Null|Null|i:3|i:300"
    );
}
