//! The `:=` assignment operator inside SELECT evaluates to the assigned
//! value and updates the user variable for later statements — including
//! chained arithmetic reading a previous assignment.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn assignment_operator_updates_and_yields() {
    let mut session = Session::new();

    // The assignment itself evaluates to the value.
    assert_eq!(try_sql(&mut session, "select @a := 5"), "i:5");
    // The variable persists across statements.
    assert_eq!(try_sql(&mut session, "select @a"), "i:5");
    // A later assignment can read the earlier one.
    assert_eq!(try_sql(&mut session, "select @b := @a + 1"), "i:6");
    assert_eq!(try_sql(&mut session, "select @b"), "i:6");
}
