//! A mid-session sql_mode change retargets the write path: under the
//! session's initial strict mode an over-long insert fails with 1406, and
//! after `set sql_mode = ''` the SAME insert truncates to the column length
//! and stores — no reconnection, no re-create.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
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
fn strict_failure_then_relaxed_success_in_one_session() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b varchar(3))").unwrap();

    let error = session
        .run("insert into t values (1, 'abcdef')")
        .expect_err("strict mode refuses the over-long value");
    assert!(error.to_string().contains("Data too long for column 'b'"), "{error}");

    session.run("set sql_mode = ''").unwrap();
    session
        .run("insert into t values (1, 'abcdef')")
        .expect("the relaxed mode truncates and stores");
    assert_eq!(rows(&mut session, "select a, b from t"), "1|'abc'");
}
