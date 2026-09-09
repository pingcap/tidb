//! Fractional durations: TIME_FORMAT renders the %f microsecond field,
//! SEC_TO_TIME preserves a half-second as fsp 1, and EXTRACT(MICROSECOND)
//! reads the six-digit tail.

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
fn microsecond_fields() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select time_format('10:20:30.123456', '%H %i %s %f')"),
        "s:10 20 30 123456"
    );

    // SEC_TO_TIME keeps the fractional part (fsp 1 = one decimal digit).
    let kept = try_sql(&mut session, "select sec_to_time(3661.5)");
    assert!(kept.contains("fsp: 1"), "{kept}");

    assert_eq!(
        try_sql(&mut session, "select extract(microsecond from '10:20:30.123456')"),
        "i:123456"
    );
}
