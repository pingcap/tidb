//! Duration-aware time functions: HOUR reads durations beyond 24 hours
//! ('25:00:00' -> 25), TIME_FORMAT renders the full duration width,
//! TIME() extracts the time part of a datetime, and MICROSECOND reads
//! the fractional tail.

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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn duration_width_and_parts() {
    let mut session = Session::new();

    // Durations exceed the 24-hour wall clock.
    assert_eq!(try_sql(&mut session, "select hour('25:00:00')"), "i:25");
    assert_eq!(
        try_sql(&mut session, "select time_format('25:30:00', '%H %i')"),
        "s:25 30"
    );

    // TIME() of a datetime extracts the clock part.
    assert_eq!(
        try_sql(&mut session, "select time_to_sec(time('2024-01-01 10:20:30'))"),
        "i:37230"
    );

    assert_eq!(
        try_sql(&mut session, "select microsecond('10:20:30.123456')"),
        "i:123456"
    );
}
