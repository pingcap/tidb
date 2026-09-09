//! The INTERVAL(N, N1, N2, ...) comparison function: returns the index of
//! the first pivot strictly greater than N (binary-search semantics), -1
//! when N is NULL.

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
fn interval_index_semantics() {
    let mut session = Session::new();

    // 5 exceeds every pivot: the index past the last one.
    assert_eq!(try_sql(&mut session, "select interval(5, 1, 2, 3)"), "i:3");
    // 2 sits between pivots 1 and 3.
    assert_eq!(try_sql(&mut session, "select interval(2, 1, 3)"), "i:1");
    // STRICTLY greater: 2 < 2 is false, so the pivot 2 does not stop the scan.
    assert_eq!(try_sql(&mut session, "select interval(2, 1, 2, 3)"), "i:2");
    // NULL N answers -1.
    assert_eq!(try_sql(&mut session, "select interval(null, 1, 2)"), "i:-1");
}
