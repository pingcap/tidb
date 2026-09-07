//! Nested views compose (a view over a view resolves both definitions).
//! `ALTER VIEW` is REFUSED by both sides: the Go oracle's hand-written
//! parser has no ALTER VIEW arm (`pkg/parser/stmt_parser.go:469-495` —
//! verified with a live parse: `GO "alter view ..." => ERR line 1 column
//! 10`), and the port rejects it identically.

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
fn nested_views_compose() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
    session.run("create view v as select a from t where a > 1").unwrap();
    session.run("create view v2 as select a from v").unwrap();

    assert_eq!(rows(&mut session, "select * from v"), "Int(2);Int(3)");
    assert_eq!(rows(&mut session, "select * from v2"), "Int(2);Int(3)");
}

#[test]
fn alter_view_is_refused_like_go() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("create view v as select a from t").unwrap();

    let error = session
        .run("alter view v as select a from t where a > 2")
        .expect_err("no ALTER VIEW in either grammar")
        .to_string();
    assert!(
        error.contains("check the manual that corresponds"),
        "{error}"
    );
}
