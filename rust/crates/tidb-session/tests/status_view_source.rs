//! SHOW TABLE STATUS for a VIEW: the row reports the view's name with a
//! NULL Engine and the `VIEW` comment (Go `fetchTableStatus`'s view arm).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
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

fn setup(session: &mut Session) {
    session.run("create table t (a int)").unwrap();
    session.run("create view v as select a from t").unwrap();
}

#[test]
fn view_status_reports_view_comment() {
    let mut session = Session::new();
    setup(&mut session);

    let row = &rows(&mut session, "show table status like 'v'")[0];
    let fields: Vec<&str> = row.split('|').map(str::trim).collect();

    assert_eq!(fields[0], "v");
    // Views have no engine; the cell is NULL.
    assert_eq!(fields[1], "Null");
    // The comment names the object kind.
    assert_eq!(*fields.last().unwrap(), "VIEW");
}
