//! CREATE OR REPLACE VIEW redefines an existing view in place (Go
//! `view_parser.go:40-42` parses `OR REPLACE`; `CreateViewStmt.OrReplace`
//! replaces the stored definition without a DROP).

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
fn or_replace_redefines_the_view() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
    session.run("create view v as select a from t where a > 2").unwrap();
    assert_eq!(rows(&mut session, "select * from v"), "Int(3)");

    session
        .run("create or replace view v as select a from t where a > 1")
        .unwrap();
    assert_eq!(rows(&mut session, "select * from v"), "Int(2);Int(3)");
}
