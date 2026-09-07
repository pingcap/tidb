//! Set-membership functions and the JSON aggregate: FIELD returns the
//! 1-based index, ELT the nth element, FIND_IN_SET the position within a
//! comma list, and JSON_OBJECTAGG builds a document from grouped rows.

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
fn membership_positions() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select field('b', 'a', 'b', 'c')"),
        "i:2"
    );
    // A missing member answers 0.
    assert_eq!(try_sql(&mut session, "select field('z', 'a', 'b', 'c')"), "i:0");
    assert_eq!(try_sql(&mut session, "select elt(2, 'a', 'b')"), "s:b");
    assert_eq!(try_sql(&mut session, "select find_in_set('b', 'a,b,c')"), "i:2");
}
