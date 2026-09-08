//! Fractional UNIX_TIMESTAMP: MySQL returns a DECIMAL whose scale tracks
//! the input's fsp — '.5' yields X.5 and '.4' yields X.4 — while an
//! integral timestamp stays an integer.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
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
fn fractional_timestamp_scale() {
    let mut session = Session::new();

    // An integral timestamp is an integer.
    assert_eq!(
        try_sql(&mut session, "select unix_timestamp('2024-01-01 00:00:00')"),
        "i:1704038400"
    );

    // A .5 fraction extends to DECIMAL scale 1; the base second is exact.
    let half = try_sql(&mut session, "select unix_timestamp('2024-01-01 00:00:00.5')");
    assert!(half.contains("d:1704038400.5"), "{half}");

    // A .4 fraction truncates the base second with .4.
    let point4 = try_sql(&mut session, "select unix_timestamp('2024-01-01 00:00:00.4')");
    assert!(point4.contains("d:1704038400.4"), "{point4}");
}
