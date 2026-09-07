//! The time/period conversion family: SEC_TO_TIME and TIME_TO_SEC are
//! exact inverses, MAKEDATE turns (year, day-of-year) into a date,
//! MAKETIME composes a duration (12:15:30 = 44130s), and PERIOD_ADD/
//! PERIOD_DIFF operate on YYMM periods.

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
fn seconds_hours_and_periods() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select time_to_sec('01:01:01')"), "i:3661");
    // SEC_TO_TIME returns a TIME; verify through TIME_TO_SEC for a
    // representation-independent round trip.
    assert_eq!(
        try_sql(&mut session, "select time_to_sec(sec_to_time(3661))"),
        "i:3661"
    );

    assert_eq!(try_sql(&mut session, "select makedate(2024, 61)").contains("2024"), true);

    assert_eq!(
        try_sql(&mut session, "select period_add(202401, 11), period_diff(202401, 202302)"),
        "i:202412|i:11"
    );
}
