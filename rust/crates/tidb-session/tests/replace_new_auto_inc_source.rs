//! REPLACE that collides allocates a NEW auto-increment id for the
//! replacement row (delete + insert semantics, matching Go) — the old id
//! is not reused.

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
fn replace_allocates_new_auto_inc_id() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, k int unique, v int)")
        .unwrap();

    session.run("insert into t (k, v) values (1, 10)").unwrap();
    session.run("replace into t (k, v) values (1, 20)").unwrap();

    // The replacement row takes id=2; id=1 is gone.
    assert_eq!(rows(&mut session, "select id, k, v from t"), "2|1|20");
}
