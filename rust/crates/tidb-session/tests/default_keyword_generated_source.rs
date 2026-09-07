//! The DEFAULT keyword in VALUES is accepted for a generated column and
//! means "use the generated value": `insert (id, a, b) values (1, 5,
//! default)` stores b = 10, not an error — alongside the explicit-value
//! refusal pinned in `generated_write_refusal_source.rs`.

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
fn default_keyword_yields_generated_value() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, a int, b int as (a * 2) stored)")
        .unwrap();

    session
        .run("insert into t (id, a, b) values (1, 5, default)")
        .unwrap();

    assert_eq!(rows(&mut session, "select id, a, b from t"), "Int(1)|Int(5)|Int(10)");
}
