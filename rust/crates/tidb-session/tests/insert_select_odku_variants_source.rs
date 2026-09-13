//! INSERT ... SELECT ... ON DUPLICATE KEY UPDATE with a PRE-EXISTING
//! duplicate: ids 2 and 3 insert while id=1 updates from the source, per
//! MySQL's 2-affected-rows-per-update convention.

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

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session.run("create table src (id int primary key, v int)").unwrap();
    session.run("create table dst (id int primary key, v int)").unwrap();
    session.run("insert into src values (1, 10), (2, 20), (3, 30)").unwrap();
    session.run("insert into dst values (1, 99)").unwrap();
}

#[test]
fn insert_select_odku_composes() {
    let mut session = Session::new();
    setup(&mut session);

    // id=1 updated from source (99 -> 10); ids 2, 3 inserted.
    assert_eq!(
        try_sql(
            &mut session,
            "insert into dst (id, v) select id, v from src on duplicate key update dst.v = src.v"
        ),
        "affected 4"
    );
    assert_eq!(rows(&mut session, "select id, v from dst order by id"), "i:1|i:10;i:2|i:20;i:3|i:30");
}
