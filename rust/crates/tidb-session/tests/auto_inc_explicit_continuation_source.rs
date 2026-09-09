//! Explicit auto-increment write rebases the allocator: the next implicit
//! insert continues from max(id) + 1 (Go's rebase-on-explicit behavior).

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

#[test]
fn implicit_insert_continues_after_explicit() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();

    session.run("insert into t (v) values (1)").unwrap();
    session.run("insert into t (id, v) values (10, 2)").unwrap();
    session.run("insert into t (v) values (3)").unwrap();

    // The implicit insert rebases to 11 (Go: max explicit id + 1).
    assert_eq!(rows(&mut session, "select id from t order by id"), "1;10;11");
}
