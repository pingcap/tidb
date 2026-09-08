//! Day-number conversions: TO_DAYS('2024-01-01') = 739251 (the standard
//! proleptic Gregorian day number), FROM_DAYS inverts it exactly, and
//! TO_SECONDS answers on a whole-day boundary.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn day_number_inversion() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select to_days('2024-01-01')"), "i:739251");

    // FROM_DAYS of the day number restores the original date (year 2000+).
    let restored = try_sql(&mut session, "select from_days(to_days('2000-01-01'))");
    assert!(restored.contains("2000 1 1"), "{restored}");

    // TO_SECONDS of a midnight date lands on a whole-day boundary.
    assert_eq!(
        try_sql(&mut session, "select to_seconds('2024-01-01') % 86400"),
        "i:0"
    );
}
