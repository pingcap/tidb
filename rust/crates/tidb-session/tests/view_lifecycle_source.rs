//! VIEW lifecycle: a view filters and projects its base table, `CREATE OR
//! REPLACE VIEW` redefines it, writes through a view are refused with TiDB's
//! "insert into view ... is not supported now", and DROP VIEW removes it.

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn view_create_read_replace_and_drop() {
    let mut session = Session::new();
    session.run("create table base_t (a int primary key, b int)").unwrap();
    session.run("insert into base_t values (1, 10), (2, 20)").unwrap();

    // The view's WHERE filters its base rows.
    session
        .run("create view v as select a, b from base_t where b > 10")
        .unwrap();
    assert_eq!(rows(&mut session, "select * from v order by a"), "2|20");

    // CREATE OR REPLACE redefines it.
    session.run("create or replace view v as select a from base_t").unwrap();
    assert_eq!(rows(&mut session, "select * from v order by a"), "1;2");

    // Writes through the view are refused.
    let error = session
        .run("insert into v values (3, 30)")
        .expect_err("a view is not writable");
    assert!(
        error.to_string().contains("insert into view v is not supported now"),
        "{error}"
    );

    // DROP VIEW removes it.
    session.run("drop view v").unwrap();
    let error = session
        .run("select * from v")
        .expect_err("the view is gone");
    assert!(error.to_string().contains("doesn't exist"), "{error}");
}
