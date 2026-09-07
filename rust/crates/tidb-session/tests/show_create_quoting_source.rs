//! SHOW CREATE TABLE identifier quoting: mixed-case and reserved-word
//! column names round-trip backquoted (`Key`, `select`), matching Go's
//! `constructResultOfShowCreateTable` quoting rule.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn reserved_and_mixed_case_names_stay_backquoted() {
    let mut session = Session::new();
    session
        .run("create table t (`Key` int primary key, `select` int)")
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("`Key`"), "{shown}");
    assert!(shown.contains("`select`"), "{shown}");
}
