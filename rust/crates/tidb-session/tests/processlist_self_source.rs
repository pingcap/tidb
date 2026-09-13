//! SHOW PROCESSLIST lists the session's own connection: the Query column
//! carries the statement text itself, Command reads `Query`, and the
//! database column reflects the current schema.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn processlist_lists_the_own_query() {
    let mut session = Session::new();
    session.run("use test").unwrap();

    let list = rows(&mut session, "show processlist");
    assert_eq!(list.len(), 1, "{list:?}");

    // The single row is the SHOW PROCESSLIST statement itself.
    let row = &list[0];
    assert!(row.ends_with("|show processlist"), "{row}");
    assert!(row.contains("|Query|"), "{row}");
    assert!(row.contains("|test|"), "{row}");
}
