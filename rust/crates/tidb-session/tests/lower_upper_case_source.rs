//! LOWER/UPPER: ASCII round-trips, accented Latin-1 letters fold (É -> é,
//! é -> É), the Greek final-sigma hazard folds with the simple mapping
//! (Σ -> σ, not ς), and NULL propagates.

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
fn case_mapping_rules() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select lower('AbC'), upper('AbC')"),
        "s:abc|s:ABC"
    );
    assert_eq!(try_sql(&mut session, "select lower('ÉÀ'), upper('éà')"), "s:éà|s:ÉÀ");
    assert_eq!(try_sql(&mut session, "select lower(null)"), "Null");
    // NOT the word-final sigma a full Unicode fold would produce.
    assert_eq!(try_sql(&mut session, "select lower('Σ')"), "s:σ");
}
