//! `SHOW WARNINGS` after a relaxed-mode truncation: the statement's
//! downgrade reaches the client as `Warning | 1406 | Data too long for
//! column 'b' at row 1`, and `@@warning_count` reports 1.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (a int primary key, b varchar(3))").unwrap();
    session.run("set sql_mode = ''").unwrap();
    session.run("insert into t values (1, 'abcdef')").unwrap();
}

#[test]
fn truncation_warning_is_visible_through_show_warnings() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        strings(&mut session, "show warnings"),
        vec!["Warning|1406|Data too long for column 'b' at row 1"]
    );
    assert_eq!(strings(&mut session, "select @@warning_count"), vec!["1"]);
}
