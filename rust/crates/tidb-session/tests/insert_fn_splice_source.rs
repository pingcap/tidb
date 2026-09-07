//! The INSERT(str, pos, len, new) splice function: pos is 1-based, pos 0
//! or beyond the length returns the input unchanged, a negative len splices
//! to the end, and NULL input propagates.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
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
fn splice_position_and_length_rules() {
    let mut session = Session::new();

    // Replace 3 chars starting at position 2.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 2, 3, 'XY')"), "s:aXYef");
    // Position 0: out of range, input unchanged.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 0, 3, 'XY')"), "s:abcdef");
    // Position beyond the end: unchanged.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 10, 3, 'XY')"), "s:abcdef");
    // Negative length: splice to the end.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 2, -1, 'XY')"), "s:aXY");
    // NULL propagates.
    assert_eq!(try_sql(&mut session, "select insert(null, 2, 3, 'X')"), "Null");
}
