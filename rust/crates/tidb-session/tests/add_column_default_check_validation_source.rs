//! Go CreateNewColumn discards inline CHECK constraints. The default still
//! backfills existing rows; an explicit ADD CONSTRAINT validates those rows.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
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
fn inline_check_is_discarded_but_explicit_check_validates_backfilled_rows() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session.run("create table t (id int primary key)").unwrap();
    session.run("insert into t values (1), (2)").unwrap();

    session
        .run("alter table t add column c int default (-1) check (c >= 0)")
        .unwrap();
    assert_eq!(strings(&mut session, "select id,c from t order by id"), "1|-1;2|-1");
    let error = session
        .run("alter table t add constraint explicit_check check (c >= 0)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 3819);
    assert_eq!(error.message, "Check constraint 'explicit_check' is violated.");
    session.run("insert into t values (3,-2)").unwrap();
    assert_eq!(strings(&mut session, "select id,c from t order by id"), "1|-1;2|-1;3|-2");
}
