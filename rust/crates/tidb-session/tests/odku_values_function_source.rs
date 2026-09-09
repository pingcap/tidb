//! The VALUES(col) function inside ON DUPLICATE KEY UPDATE refers to the
//! NEW row's would-be value: `... values (1, 99) on duplicate key update
//! v = values(v) + 1` stores 100.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn values_function_reads_the_new_row() {
    let mut session = Session::new();
    session.run("create table t (id int primary key, v int)").unwrap();
    session.run("insert into t values (1, 10)").unwrap();

    session
        .run("insert into t (id, v) values (1, 99) on duplicate key update v = values(v) + 1")
        .unwrap();

    // The stored v comes from the NEW row's 99, not the old 10.
    assert_eq!(rows(&mut session, "select id, v from t"), "Int(1)|Int(100)");
}
