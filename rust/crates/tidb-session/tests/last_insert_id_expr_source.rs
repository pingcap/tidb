//! LAST_INSERT_ID(expr) sets the session's last-insert-id AND evaluates to
//! the value; a later LAST_INSERT_ID() reads it back.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::UInt(v) => format!("u:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

#[test]
fn set_and_read_back() {
    let mut session = Session::new();

    // The argument form both stores and returns the value (unsigned).
    assert_eq!(try_sql(&mut session, "select last_insert_id(77)"), "u:77");

    // The next statement reads the stored value.
    assert_eq!(try_sql(&mut session, "select last_insert_id()"), "u:77");
}
